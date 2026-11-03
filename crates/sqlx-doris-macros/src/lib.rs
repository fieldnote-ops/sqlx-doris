//! Compile-time SQLx integration for Doris.
#[cfg(feature = "query")]
use proc_macro::TokenStream;
#[cfg(any(feature = "query", test))]
use proc_macro2::TokenStream as Tokens;
#[cfg(any(feature = "query", test))]
use quote::quote;

#[cfg(feature = "query")]
fn driver_crate() -> String {
    match proc_macro_crate::crate_name("sqlx-doris") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => name,
        _ => "sqlx_doris".into(),
    }
}
#[cfg(test)]
fn paths(tokens: Tokens, driver_crate: &str) -> Tokens {
    paths_with_inputs(tokens, driver_crate, &Default::default(), None)
}
#[cfg(any(feature = "query", test))]
fn paths_with_inputs(
    tokens: Tokens,
    driver_crate: &str,
    inputs: &std::collections::HashSet<String>,
    record: Option<&syn::Type>,
) -> Tokens {
    use syn::visit_mut::VisitMut;
    struct Rewrite<'a> {
        driver_crate: syn::Ident,
        inputs: &'a std::collections::HashSet<String>,
        record: Option<&'a syn::Type>,
    }
    impl VisitMut for Rewrite<'_> {
        fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
            if !self.inputs.contains(&quote!(#expr).to_string()) {
                syn::visit_mut::visit_expr_mut(self, expr);
            }
        }
        fn visit_type_mut(&mut self, ty: &mut syn::Type) {
            if !self
                .record
                .is_some_and(|r| quote!(#r).to_string() == quote!(#ty).to_string())
            {
                syn::visit_mut::visit_type_mut(self, ty);
            }
        }
        fn visit_expr_struct_mut(&mut self, expr: &mut syn::ExprStruct) {
            let original = syn::TypePath {
                qself: expr.qself.clone(),
                path: expr.path.clone(),
            };
            let preserve = self
                .record
                .is_some_and(|r| quote!(#r).to_string() == quote!(#original).to_string());
            syn::visit_mut::visit_expr_struct_mut(self, expr);
            if preserve {
                expr.qself = original.qself;
                expr.path = original.path;
            }
        }
        fn visit_item_use_mut(&mut self, item: &mut syn::ItemUse) {
            if item.leading_colon.is_some()
                && let syn::UseTree::Path(path) = &item.tree
            {
                let root = &self.driver_crate;
                let rest = &path.tree;
                if path.ident == "sqlx_doris" {
                    item.tree = syn::parse_quote!(#root::#rest);
                }
            }
            syn::visit_mut::visit_item_use_mut(self, item);
        }
        fn visit_path_mut(&mut self, path: &mut syn::Path) {
            if let Some(first) = path.segments.first()
                && ((path.leading_colon.is_some() && first.ident == "sqlx_doris")
                    || (first.ident == "sqlx"
                        && path
                            .segments
                            .get(1)
                            .is_some_and(|s| s.ident == "__spec_error")))
            {
                let sqlx = first.ident == "sqlx";
                let rest = path.segments.iter().skip(1).cloned().collect::<Vec<_>>();
                let root = &self.driver_crate;
                let mut new: syn::Path = if sqlx {
                    syn::parse_quote!(::sqlx)
                } else {
                    syn::parse_quote!(::#root)
                };
                new.segments.extend(rest);
                *path = new;
            }
            syn::visit_mut::visit_path_mut(self, path);
        }
    }
    let mut rewrite = Rewrite {
        inputs,
        record,
        driver_crate: syn::parse_str(driver_crate)
            .unwrap_or_else(|_| syn::Ident::new_raw(driver_crate, proc_macro2::Span::call_site())),
    };
    if let Ok(mut expr) = syn::parse2::<syn::Expr>(tokens.clone()) {
        rewrite.visit_expr_mut(&mut expr);
        quote!(#expr)
    } else if let Ok(mut file) = syn::parse2::<syn::File>(tokens.clone()) {
        rewrite.visit_file_mut(&mut file);
        quote!(#file)
    } else {
        tokens
    }
}
#[cfg(feature = "query")]
fn expand_query(input: TokenStream, kind: &str, file: bool) -> TokenStream {
    if !matches!(proc_macro_crate::crate_name("sqlx"), Ok(proc_macro_crate::FoundCrate::Name(name)) if name == "sqlx")
    {
        return quote!(::core::compile_error!("Doris query macros require upstream sqlx 0.9 as a direct dependency named sqlx (default-features = false, features = [\"derive\"] or [\"macros\"]); import Doris macros from sqlx_doris")).into();
    }
    let input: Tokens = input.into();
    let source = if file {
        quote!(source_file)
    } else {
        quote!(source)
    };
    let mut protected = std::collections::HashSet::new();
    let mut record_type = None;
    let parsed = if kind == "as" {
        let p = syn::parse2::<AsInput>(input);
        match p {
            Ok(p) => {
                let record = p.record;
                let sql = p.sql;
                let args = p.args;
                if let Err(e) = preserve_arguments(&args, &mut protected) {
                    return e.to_compile_error().into();
                }
                record_type = Some(record.clone());
                quote!(record=#record,#source=#sql,args=[#args])
            }
            Err(e) => return e.to_compile_error().into(),
        }
    } else {
        let p = syn::parse2::<Input>(input);
        match p {
            Ok(p) => {
                let sql = p.sql;
                let args = p.args;
                if let Err(e) = preserve_arguments(&args, &mut protected) {
                    return e.to_compile_error().into();
                }
                if kind == "scalar" {
                    quote!(scalar=_,#source=#sql,args=[#args])
                } else {
                    quote!(#source=#sql,args=[#args])
                }
            }
            Err(e) => return e.to_compile_error().into(),
        }
    };
    let parsed = match syn::parse2::<sqlx_macros_core::query::QueryMacroInput>(parsed) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    const DRIVER: sqlx_macros_core::query::QueryDriver =
        sqlx_macros_core::query::QueryDriver::new::<sqlx_doris_core::Doris>();
    match sqlx_macros_core::query::expand_input(parsed, [&DRIVER]) {
        Ok(tokens) => {
            paths_with_inputs(tokens, &driver_crate(), &protected, record_type.as_ref()).into()
        }
        Err(e) => {
            let message = e
                .to_string()
                .replace("cargo sqlx prepare", "cargo sqlx-doris prepare");
            quote!(::core::compile_error!(#message)).into()
        }
    }
}
#[cfg(feature = "query")]
fn preserve_arguments(
    args: &Tokens,
    inputs: &mut std::collections::HashSet<String>,
) -> syn::Result<()> {
    use syn::parse::Parser;
    fn add(expr: &syn::Expr, inputs: &mut std::collections::HashSet<String>) {
        inputs.insert(quote!(#expr).to_string());
        match expr {
            syn::Expr::Group(g) => add(&g.expr, inputs),
            syn::Expr::Cast(c) if matches!(*c.ty, syn::Type::Infer(_)) => add(&c.expr, inputs),
            _ => {}
        }
    }
    for expr in syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated
        .parse2(args.clone())?
    {
        add(&expr, inputs);
    }
    Ok(())
}
#[cfg(feature = "query")]
struct Input {
    sql: syn::Expr,
    args: Tokens,
}
#[cfg(feature = "query")]
impl syn::parse::Parse for Input {
    fn parse(p: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let sql = p.parse()?;
        let args = if p.is_empty() {
            Tokens::new()
        } else {
            p.parse::<syn::Token![,]>()?;
            p.parse()?
        };
        Ok(Self { sql, args })
    }
}
#[cfg(feature = "query")]
struct AsInput {
    record: syn::Type,
    sql: syn::Expr,
    args: Tokens,
}
#[cfg(feature = "query")]
impl syn::parse::Parse for AsInput {
    fn parse(p: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let record = p.parse()?;
        p.parse::<syn::Token![,]>()?;
        let Input { sql, args } = p.parse()?;
        Ok(Self { record, sql, args })
    }
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a query read from a SQL file and infer its output record.
pub fn query_file(input: TokenStream) -> TokenStream {
    expand_query(input, "query", true)
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a SQL file and map its columns into a named record.
pub fn query_file_as(input: TokenStream) -> TokenStream {
    expand_query(input, "as", true)
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a SQL file returning one column.
pub fn query_file_scalar(input: TokenStream) -> TokenStream {
    expand_query(input, "scalar", true)
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a query and map its columns into a named record.
pub fn query_as(input: TokenStream) -> TokenStream {
    expand_query(input, "as", false)
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a query returning one column.
pub fn query_scalar(input: TokenStream) -> TokenStream {
    expand_query(input, "scalar", false)
}
#[cfg(feature = "query")]
#[proc_macro]
/// Check a query and infer its output record.
pub fn query(input: TokenStream) -> TokenStream {
    query_impl(input)
}
#[cfg(feature = "query")]
fn query_impl(input: TokenStream) -> TokenStream {
    expand_query(input, "query", false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renamed_driver_crate_preserves_associated_types_and_user_identifiers() {
        let tokens = quote!({
            use ::sqlx::Arguments;
            let sqlx_doris = "::sqlx::do_not_rewrite_strings";
            let error = sqlx::__spec_error!(e);
            let sqlx: <DB as ::sqlx::database::Database>::Arguments<'_> = Default::default();
            let _: ::sqlx_doris::types::LargeInt = todo!();
            (sqlx, sqlx_doris)
        });
        let actual = paths(tokens, "dbkit");
        let expected = quote!({
            use ::sqlx::Arguments;
            let sqlx_doris = "::sqlx::do_not_rewrite_strings";
            let error = ::sqlx::__spec_error!(e);
            let sqlx: <DB as ::sqlx::database::Database>::Arguments<'_> = Default::default();
            let _: ::dbkit::types::LargeInt = todo!();
            (sqlx, sqlx_doris)
        });
        assert_eq!(actual.to_string(), expected.to_string());
    }
    #[test]
    fn caller_expressions_and_record_paths_keep_their_original_meaning() {
        let caller: syn::Expr = syn::parse_quote!({
            let _e = ::sqlx_doris::operations::Error::InvalidInput("value".into());
            42_i64
        });
        let inputs = [quote!(#caller).to_string()].into_iter().collect();
        let record: syn::Type = syn::parse_quote!(::sqlx_doris::operations::Record);
        let tokens = quote!({
            use ::sqlx::Row;
            let _: ::sqlx_doris::DorisRow = todo!();
            let arg = &(#caller);
            let _: #record = #record { value: ::sqlx::query("x") };
        });
        let rewritten = paths_with_inputs(tokens, "dbkit", &inputs, Some(&record));
        let expected = quote!({
            use ::sqlx::Row;
            let _: ::dbkit::DorisRow = todo!();
            let arg = &(#caller);
            let _: #record = #record { value: ::sqlx::query("x") };
        });
        assert_eq!(rewritten.to_string(), expected.to_string());
    }
}
