//! Private files and build paths on a physically external, live mounted volume.
use crate::failure::Result;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};
// The physical-disk probe is macOS-only; other Unix hosts use the mount device.
#[cfg(target_os = "macos")]
use std::process::Command;

/// One physical-volume attestation per operation; each file still checks the
/// live mount/device, private permissions and opened inode. Never cached globally.
pub struct PrivateFiles {
    pub(crate) volume: PathBuf,
    #[cfg(unix)]
    pub(crate) device: u64,
}
impl PrivateFiles {
    pub fn new() -> Result<Self> {
        let volume = external_volume()?;
        #[cfg(unix)]
        let device = {
            use std::os::unix::fs::MetadataExt;
            volume.metadata()?.dev()
        };
        Ok(Self {
            volume,
            #[cfg(unix)]
            device,
        })
    }
    pub fn check(&self, path: &Path) -> Result<()> {
        if !path.starts_with(&self.volume) {
            return Err("private file escapes external volume".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if self.volume.metadata()?.dev() != self.device
                || self
                    .volume
                    .parent()
                    .ok_or("invalid external mount")?
                    .metadata()?
                    .dev()
                    == self.device
                || path.metadata()?.dev() != self.device
            {
                return Err(
                    "external mount/device changed or file resides on another device".into(),
                );
            }
        }
        Ok(())
    }
    pub fn open(&self, path: &Path) -> Result<std::fs::File> {
        if path
            .components()
            .any(|v| v == std::path::Component::ParentDir)
        {
            return Err("credential paths cannot contain parent-directory components".into());
        }
        let meta = std::fs::symlink_metadata(path)?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err("credential file must be a regular file, not a symlink".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(
                    "credential file must not be accessible by group or others (use 0600)".into(),
                );
            }
        }
        let resolved = path.canonicalize()?;
        self.check(&resolved)?;
        let file = std::fs::File::open(resolved)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let opened = file.metadata()?;
            if opened.dev() != self.device
                || (opened.dev(), opened.ino()) != (meta.dev(), meta.ino())
            {
                return Err("credential file changed during validation".into());
            }
        }
        Ok(file)
    }
}
pub fn external(path: &Path, volume: &Path) -> Result<PathBuf> {
    // Check existing ancestors before creating directories or following links.
    if path
        .components()
        .any(|c| c == std::path::Component::ParentDir)
    {
        return Err("external paths cannot contain parent-directory components".into());
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    while !ancestor.exists() {
        ancestor = ancestor.parent().ok_or("invalid external path")?;
    }
    if !ancestor.canonicalize()?.starts_with(volume) {
        return Err(format!("path escapes external volume: {}", path.display()).into());
    }
    std::fs::create_dir_all(&absolute)?;
    let resolved = absolute.canonicalize()?;
    if !resolved.starts_with(volume) {
        return Err("resolved path escapes external volume".into());
    }
    Ok(resolved)
}
pub fn external_volume() -> Result<PathBuf> {
    let configured = std::env::var("SQLX_DORIS_EXTERNAL_VOLUME")
        .map_err(|_| "SQLX_DORIS_EXTERNAL_VOLUME must name a mounted external volume")?;
    let volume = Path::new(&configured).canonicalize()?;
    #[cfg(target_os = "macos")]
    {
        let probe = Command::new("diskutil")
            .args(["info", "-plist"])
            .arg(&volume)
            .output()?;
        let plist = String::from_utf8(probe.stdout)?
            .split_whitespace()
            .collect::<String>();
        if !probe.status.success() || !plist.contains("<key>Internal</key><false/>") {
            return Err("volume must be on a physical external disk".into());
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if volume.metadata()?.dev() == Path::new("/").metadata()?.dev()
            || volume.metadata()?.dev()
                == volume
                    .parent()
                    .ok_or("invalid mount point")?
                    .metadata()?
                    .dev()
        {
            return Err("external volume must be separately mounted".into());
        }
    }
    Ok(volume)
}

impl PrivateFiles {
    pub fn read_limited(&self, path: &Path, limit: usize) -> Result<Vec<u8>> {
        let file = self.open(path)?;
        if file.metadata()?.len() > limit as u64 {
            return Err("private file exceeds size limit".into());
        }
        let mut bytes = Vec::new();
        file.take((limit + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err("private file exceeds size limit".into());
        }
        Ok(bytes)
    }

    pub fn write_new(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let create = (|| -> Result<(std::fs::File, PathBuf)> {
            if path
                .components()
                .any(|part| part == std::path::Component::ParentDir)
            {
                return Err("private output path cannot contain parent components".into());
            }
            let absolute = if path.is_absolute() {
                path.to_owned()
            } else {
                std::env::current_dir()?.join(path)
            };
            let parent = absolute
                .parent()
                .ok_or("private output parent missing")?
                .canonicalize()?;
            self.check(&parent)?;
            let path = parent.join(
                absolute
                    .file_name()
                    .ok_or("private output filename missing")?,
            );
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            #[cfg(not(unix))]
            return Err("private output requires Unix permission enforcement".into());
            let file = options.open(&path)?;
            self.check(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let opened = file.metadata()?;
                let named = std::fs::symlink_metadata(&path)?;
                if named.file_type().is_symlink()
                    || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
                {
                    return Err("private output changed while being opened".into());
                }
            }
            Ok((file, parent))
        })();
        let (mut file, parent) = create.map_err(|cause| {
            crate::failure::wrap(
                2,
                "output must be a new private file on the attested external volume",
                cause,
            )
        })?;
        let persist = (|| -> std::io::Result<()> {
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        persist.map_err(|cause| {
            crate::failure::wrap(
                7,
                "local file write or sync is unconfirmed; retain and inspect the file before reuse",
                Box::new(cause),
            )
        })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn private_files_are_bounded_never_overwritten_and_checked_on_each_access() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let files = PrivateFiles::new().unwrap();
        let root = std::env::temp_dir().join(format!("private-files-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("receipt.json");
        files.write_new(&path, b"original").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(files.read_limited(&path, 8).unwrap(), b"original");
        assert!(files.read_limited(&path, 7).is_err());
        let rejected = files.write_new(&path, b"replacement").unwrap_err();
        assert_eq!(
            rejected
                .downcast_ref::<crate::failure::Failure>()
                .unwrap()
                .code(),
            2
        );
        assert_eq!(files.read_limited(&path, 8).unwrap(), b"original");
        let alias = root.join("alias");
        symlink(&path, &alias).unwrap();
        assert!(files.read_limited(&alias, 8).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(files.read_limited(&path, 8).is_err());
        assert!(
            files
                .write_new(&root.join("..").join("escaped"), b"bad")
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
