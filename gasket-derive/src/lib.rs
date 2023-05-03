use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::{parse_macro_input, Attribute, DataStruct, DeriveInput, Expr, Lit, MetaNameValue, Token};

use syn::Field;

fn expect_struct(input: &DeriveInput) -> syn::Result<&DataStruct> {
    match &input.data {
        syn::Data::Struct(x) => Ok(x),
        _ => {
            let err = syn::Error::new_spanned(&input.ident, "you need to derive from a struct");
            Err(err)
        }
    }
}

fn has_attribute(attrs: &[Attribute], attr_name: &str) -> bool {
    attrs
        .iter()
        .any(|attr| attr.meta.path().is_ident(attr_name))
}

fn fields_with_attribute<'a>(data: &'a DataStruct, attr_name: &'static str) -> Vec<&'a Field> {
    data.fields
        .iter()
        .filter(|field| has_attribute(&field.attrs, attr_name))
        .collect()
}

struct StageArgs {
    name: Option<String>,
    unit: syn::Type,
    worker: syn::Type,
}

impl syn::parse::Parse for StageArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut unit = None;
        let mut worker = None;

        while !input.is_empty() {
            let name_value: MetaNameValue = input.parse()?;
            let MetaNameValue { path, value, .. } = name_value;

            if path.is_ident("name") {
                if let Expr::Lit(expr) = value {
                    if let Lit::Str(x) = expr.lit {
                        name = Some(x.value());
                    }
                }
            } else if path.is_ident("unit") {
                if let Expr::Lit(expr) = value {
                    if let Lit::Str(x) = expr.lit {
                        unit = Some(x.parse()?);
                    }
                }
            } else if path.is_ident("worker") {
                if let Expr::Lit(expr) = value {
                    if let Lit::Str(x) = expr.lit {
                        worker = Some(x.parse()?);
                    }
                }
            } else {
                return Err(input.error("Unexpected argument"));
            }

            // Ignore commas between arguments
            let _ = input.parse::<Option<Token![,]>>();
        }

        Ok(Self {
            name,
            unit: unit.ok_or_else(|| input.error("Missing unit type"))?,
            worker: worker.ok_or_else(|| input.error("Missing worker type"))?,
        })
    }
}

fn matches_type(type_path: &syn::TypePath, target_type: &str) -> bool {
    type_path
        .path
        .segments
        .last()
        .map_or(false, |segment| segment.ident == target_type)
}

fn expand_metric_registration(struct_: &DataStruct) -> syn::Result<Vec<TokenStream>> {
    let metrics_code: syn::Result<Vec<_>> = fields_with_attribute(struct_, "metric")
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();

            match &field.ty {
                syn::Type::Path(x) if matches_type(x, "Counter") => {
                    let q = quote! {
                        registry.track_counter(stringify!(#field_name), &self.#field_name);
                    };

                    Ok(q)
                }
                syn::Type::Path(x) if matches_type(x, "Gauge") => {
                    let q = quote! {
                        registry.track_gauge(stringify!(#field_name), &self.#field_name);
                    };

                    Ok(q)
                }
                _ => Err(syn::Error::new_spanned(
                    field,
                    "unknown return type for metric",
                )),
            }
        })
        .collect();

    metrics_code
}

fn expand_stage_impl(input: DeriveInput) -> TokenStream {
    let struct_ = match expect_struct(&input) {
        Ok(x) => x,
        Err(err) => return TokenStream::from(err.to_compile_error()),
    };

    let metrics_code = match expand_metric_registration(struct_) {
        Ok(x) => x,
        Err(err) => return err.to_compile_error(),
    };

    let stage_args: StageArgs = input
        .attrs
        .iter()
        .find(|a| a.meta.path().is_ident("stage"))
        .unwrap()
        .parse_args()
        .unwrap();

    let name = input.ident;

    let hri = stage_args.name.unwrap_or_else(|| stringify!(name).into());
    let unit_type = stage_args.unit;
    let worker_type = stage_args.worker;

    quote! {
        impl gasket::framework::Stage for #name {
            type Unit = #unit_type;
            type Worker = #worker_type;

            fn name(&self) -> &str {
                #hri
            }

            fn metrics(&self) -> gasket::metrics::Registry {
                let mut registry = gasket::metrics::Registry::default();

                #(#metrics_code)*

                registry
            }
        }
    }
}

#[proc_macro_derive(Stage, attributes(stage, metric))]
pub fn stage_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = expand_stage_impl(input);
    proc_macro::TokenStream::from(expanded)
}
