use {
    proc_macro2::{Span, TokenStream},
    quote::{format_ident, quote},
    std::collections::{BTreeMap, BTreeSet},
    syn::{
        Block, Expr, Ident, ItemFn, LitInt, LitStr, Pat, Stmt, Token, parenthesized,
        parse::{Parse, ParseStream},
        parse_quote,
        visit_mut::{self, VisitMut},
    },
};

struct Field {
    name: Ident,
    value: Expr,
    format: Option<char>,
}

impl Parse for Field {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![=]>()?;
        let format = if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            Some('?')
        } else if input.peek(Token![%]) {
            input.parse::<Token![%]>()?;
            Some('%')
        } else {
            None
        };
        Ok(Self {
            name,
            value: input.parse()?,
            format,
        })
    }
}

impl Field {
    fn value(&self) -> TokenStream {
        let value = &self.value;
        match self.format {
            Some('?') => quote!(tracing::field::debug(&(#value))),
            Some('%') => quote!(tracing::field::display(&(#value))),
            _ => quote!(&(#value)),
        }
    }
}

fn fields(input: ParseStream) -> syn::Result<Vec<Field>> {
    let content;
    parenthesized!(content in input);
    Ok(content
        .parse_terminated(Field::parse, Token![,])?
        .into_iter()
        .collect())
}

#[derive(Clone)]
struct Selector {
    binding: Ident,
    occurrence: Option<usize>,
    all: bool,
    is_loop: bool,
}

impl Selector {
    fn new(binding: Ident, is_loop: bool) -> Self {
        Self {
            binding,
            occurrence: None,
            all: false,
            is_loop,
        }
    }

    fn option(&mut self, key: &Ident, input: ParseStream) -> syn::Result<()> {
        if key == "all" && !self.all && self.occurrence.is_none() {
            self.all = true;
        } else if key == "occurrence" && !self.all && self.occurrence.is_none() {
            input.parse::<Token![=]>()?;
            let n = input.parse::<LitInt>()?;
            let value = n.base10_parse()?;
            if value == 0 {
                return Err(syn::Error::new_spanned(n, "occurrence starts at 1"));
            }
            self.occurrence = Some(value);
        } else {
            return Err(syn::Error::new_spanned(
                key,
                "expected one of `occurrence = N` or `all`",
            ));
        }
        Ok(())
    }

    fn locations(&self, block: &mut Block) -> syn::Result<Vec<Location>> {
        let mut search = Search {
            selector: self,
            block_id: 0,
            found: Vec::new(),
        };
        search.visit_block_mut(block);
        let found = search.found;
        if let Some(n) = self.occurrence {
            return found.get(n - 1).copied().map(|v| vec![v]).ok_or_else(|| {
                syn::Error::new_spanned(&self.binding, "requested occurrence was not found")
            });
        }
        if found.is_empty() || (found.len() > 1 && !self.all) {
            return Err(syn::Error::new_spanned(
                &self.binding,
                if found.is_empty() {
                    "observation target was not found"
                } else {
                    "ambiguous observation target; specify `occurrence = N` or `all`"
                },
            ));
        }
        Ok(found)
    }
}

struct Hook {
    selector: Selector,
    fields: Vec<Field>,
}
struct Point {
    selector: Selector,
    after: bool,
}
struct Counter {
    selector: Selector,
    increment: Option<Selector>,
    field: Ident,
}

fn point(input: ParseStream) -> syn::Result<Point> {
    let kind: Ident = input.parse()?;
    if kind != "before_let" && kind != "after_let" {
        return Err(syn::Error::new_spanned(
            kind,
            "expected before_let(...) or after_let(...)",
        ));
    }
    let content;
    parenthesized!(content in input);
    let mut selector = Selector::new(content.parse()?, false);
    while !content.is_empty() {
        content.parse::<Token![,]>()?;
        if content.is_empty() {
            break;
        }
        let key = content.parse()?;
        selector.option(&key, &content)?;
    }
    if selector.all {
        return Err(syn::Error::new_spanned(
            kind,
            "range endpoints must select exactly one statement",
        ));
    }
    Ok(Point {
        selector,
        after: kind == "after_let",
    })
}

#[derive(Default)]
struct Args {
    name: Option<LitStr>,
    level: Option<LitStr>,
    target: Option<LitStr>,
    fields: Vec<Field>,
    hooks: Vec<Hook>,
    counters: Vec<Counter>,
    start: Option<Point>,
    end: Option<Point>,
    record: Vec<Field>,
    on_ok: Option<(Ident, Vec<Field>)>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Self::default();
        let mut seen = BTreeSet::new();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let name = key.to_string();
            if !matches!(name.as_str(), "after_let" | "after_loop" | "count_loop")
                && !seen.insert(name.clone())
            {
                return Err(syn::Error::new_spanned(key, "duplicate observation option"));
            }
            match name.as_str() {
                "name" | "level" | "target" => {
                    input.parse::<Token![=]>()?;
                    let value = input.parse()?;
                    match name.as_str() {
                        "name" => args.name = Some(value),
                        "level" => args.level = Some(value),
                        _ => args.target = Some(value),
                    }
                }
                "fields" => args.fields = fields(input)?,
                "record" => args.record = fields(input)?,
                "start" | "end" => {
                    input.parse::<Token![=]>()?;
                    let value = point(input)?;
                    if name == "start" {
                        args.start = Some(value);
                    } else {
                        args.end = Some(value);
                    }
                }
                "after_let" | "after_loop" => {
                    let content;
                    parenthesized!(content in input);
                    let mut selector = Selector::new(content.parse()?, name == "after_loop");
                    let mut record = None;
                    while !content.is_empty() {
                        content.parse::<Token![,]>()?;
                        if content.is_empty() {
                            break;
                        }
                        let key: Ident = content.parse()?;
                        if key == "record" && record.is_none() {
                            record = Some(fields(&content)?);
                        } else {
                            selector.option(&key, &content)?;
                        }
                    }
                    args.hooks.push(Hook {
                        selector,
                        fields: record
                            .ok_or_else(|| syn::Error::new_spanned(key, "missing record(...)"))?,
                    });
                }
                "on_ok" => {
                    let content;
                    parenthesized!(content in input);
                    let binding = content.parse()?;
                    content.parse::<Token![,]>()?;
                    let key: Ident = content.parse()?;
                    if key != "record" {
                        return Err(syn::Error::new_spanned(key, "expected record(...)"));
                    }
                    let record = fields(&content)?;
                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    }
                    if !content.is_empty() {
                        return Err(content.error("unexpected on_ok option"));
                    }
                    args.on_ok = Some((binding, record));
                }
                "count_loop" => {
                    let content;
                    parenthesized!(content in input);
                    let mut binding = None;
                    let mut field = None;
                    let mut increment = None;
                    let mut occurrence = None;
                    let mut options = BTreeSet::new();
                    while !content.is_empty() {
                        let key: Ident = content.parse()?;
                        if !options.insert(key.to_string()) {
                            return Err(syn::Error::new_spanned(key, "duplicate counter option"));
                        }
                        content.parse::<Token![=]>()?;
                        match key.to_string().as_str() {
                            "binding" => binding = Some(content.parse()?),
                            "field" => field = Some(content.parse()?),
                            "occurrence" => {
                                let n: LitInt = content.parse()?;
                                let value = n.base10_parse()?;
                                if value == 0 {
                                    return Err(syn::Error::new_spanned(
                                        n,
                                        "occurrence starts at 1",
                                    ));
                                }
                                occurrence = Some(value);
                            }
                            "increment" => {
                                let p = point(&content)?;
                                if !p.after {
                                    return Err(syn::Error::new_spanned(
                                        key,
                                        "increment requires after_let(...)",
                                    ));
                                }
                                increment = Some(p.selector);
                            }
                            _ => {
                                return Err(syn::Error::new_spanned(
                                    key,
                                    "unsupported counter option",
                                ));
                            }
                        }
                        if !content.is_empty() {
                            content.parse::<Token![,]>()?;
                        }
                    }
                    let mut selector = Selector::new(
                        binding.ok_or_else(|| syn::Error::new_spanned(&key, "missing binding"))?,
                        true,
                    );
                    selector.occurrence = occurrence;
                    args.counters.push(Counter {
                        selector,
                        increment,
                        field: field
                            .ok_or_else(|| syn::Error::new_spanned(key, "missing field"))?,
                    });
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "unsupported observation option",
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Location {
    block: usize,
    statement: usize,
}

fn binds(pat: &Pat, name: &Ident) -> bool {
    match pat {
        Pat::Ident(p) => p.ident == *name || p.subpat.as_ref().is_some_and(|(_, p)| binds(p, name)),
        Pat::Tuple(p) => p.elems.iter().any(|p| binds(p, name)),
        Pat::TupleStruct(p) => p.elems.iter().any(|p| binds(p, name)),
        Pat::Struct(p) => p.fields.iter().any(|p| binds(&p.pat, name)),
        Pat::Slice(p) => p.elems.iter().any(|p| binds(p, name)),
        Pat::Reference(p) => binds(&p.pat, name),
        Pat::Type(p) => binds(&p.pat, name),
        Pat::Paren(p) => binds(&p.pat, name),
        _ => false,
    }
}

struct Search<'a> {
    selector: &'a Selector,
    block_id: usize,
    found: Vec<Location>,
}

impl VisitMut for Search<'_> {
    fn visit_item_mut(&mut self, _: &mut syn::Item) {}
    fn visit_expr_closure_mut(&mut self, _: &mut syn::ExprClosure) {}
    fn visit_expr_async_mut(&mut self, _: &mut syn::ExprAsync) {}
    fn visit_block_mut(&mut self, block: &mut Block) {
        let id = self.block_id;
        self.block_id += 1;
        for (i, stmt) in block.stmts.iter_mut().enumerate() {
            let matched = match stmt {
                Stmt::Local(local) if !self.selector.is_loop => {
                    local.init.is_some() && binds(&local.pat, &self.selector.binding)
                }
                Stmt::Expr(Expr::ForLoop(expr), _) if self.selector.is_loop => {
                    binds(&expr.pat, &self.selector.binding)
                }
                _ => false,
            };
            if matched {
                self.found.push(Location {
                    block: id,
                    statement: i,
                });
            }
            visit_mut::visit_stmt_mut(self, stmt);
        }
    }
}

#[derive(Default)]
struct Edits {
    before: BTreeMap<Location, Vec<Stmt>>,
    after: BTreeMap<Location, Vec<Stmt>>,
    block_id: usize,
}

impl Edits {
    fn add(&mut self, location: Location, after: bool, tokens: &TokenStream) -> syn::Result<()> {
        let block: Block = syn::parse2(quote!({ #tokens }))?;
        let map = if after {
            &mut self.after
        } else {
            &mut self.before
        };
        map.entry(location).or_default().extend(block.stmts);
        Ok(())
    }
}

impl VisitMut for Edits {
    fn visit_item_mut(&mut self, _: &mut syn::Item) {}
    fn visit_expr_closure_mut(&mut self, _: &mut syn::ExprClosure) {}
    fn visit_expr_async_mut(&mut self, _: &mut syn::ExprAsync) {}
    fn visit_block_mut(&mut self, block: &mut Block) {
        let id = self.block_id;
        self.block_id += 1;
        let mut stmts = Vec::new();
        for (i, mut stmt) in std::mem::take(&mut block.stmts).into_iter().enumerate() {
            self.visit_stmt_mut(&mut stmt);
            let location = Location {
                block: id,
                statement: i,
            };
            stmts.extend(self.before.remove(&location).unwrap_or_default());
            stmts.push(stmt);
            stmts.extend(self.after.remove(&location).unwrap_or_default());
        }
        block.stmts = stmts;
    }
}

fn record(fields: &[Field], span: &Ident) -> TokenStream {
    let writes = fields.iter().map(|field| {
        let name = field.name.to_string();
        let value = field.value();
        quote!(#span.record(#name, #value);)
    });
    quote!(if !#span.is_disabled() { #(#writes)* })
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: Args = syn::parse2(attr)?;
    let mut function: ItemFn = syn::parse2(item)?;
    if function.sig.asyncness.is_some() || function.sig.constness.is_some() {
        return Err(syn::Error::new_spanned(
            &function.sig,
            "observe currently supports synchronous, non-const functions",
        ));
    }
    let name = args
        .name
        .as_ref()
        .ok_or_else(|| syn::Error::new(Span::call_site(), "missing name = \"...\""))?;
    let level = args
        .level
        .as_ref()
        .map_or_else(|| "debug".into(), LitStr::value);
    if !matches!(
        level.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        return Err(syn::Error::new_spanned(
            args.level,
            "unsupported span level",
        ));
    }
    let level = format_ident!("{}", level.to_uppercase());
    let target = args
        .target
        .as_ref()
        .map_or_else(|| quote!("gluesql"), |v| quote!(#v));
    let span = Ident::new("__gluesql_observe_span", Span::mixed_site());
    let entered = Ident::new("__gluesql_observe_entered", Span::mixed_site());
    let mut names = BTreeSet::new();
    for field in &args.fields {
        if !names.insert(field.name.to_string()) {
            return Err(syn::Error::new_spanned(
                &field.name,
                "duplicate initial field",
            ));
        }
    }
    for field in args
        .hooks
        .iter()
        .flat_map(|h| &h.fields)
        .chain(&args.record)
        .chain(args.on_ok.iter().flat_map(|(_, f)| f))
    {
        names.insert(field.name.to_string());
    }
    for counter in &args.counters {
        names.insert(counter.field.to_string());
    }
    let declarations = names.iter().map(|n| {
        let ident = Ident::new(n, Span::call_site());
        quote!(#ident = tracing::field::Empty)
    });
    let initial = record(&args.fields, &span);
    let start = quote! {
        let #span = tracing::span!(target: #target, tracing::Level::#level, #name, #(#declarations),*);
        #initial
        let #entered = #span.enter();
    };
    if args.start.is_some() != args.end.is_some() || (!args.record.is_empty() && args.end.is_none())
    {
        return Err(syn::Error::new_spanned(
            name,
            "start and end must be paired; record requires a range",
        ));
    }
    if args.start.is_some()
        && (!args.hooks.is_empty() || !args.counters.is_empty() || args.on_ok.is_some())
    {
        return Err(syn::Error::new_spanned(
            name,
            "range observations support fields and record; use a separate observe attribute for other hooks",
        ));
    }
    let mut edits = Edits::default();
    if let (Some(start_point), Some(end_point)) = (&args.start, &args.end) {
        let a = start_point.selector.locations(&mut function.block)?[0];
        let b = end_point.selector.locations(&mut function.block)?[0];
        if a.block != b.block || (a.statement, start_point.after) >= (b.statement, end_point.after)
        {
            return Err(syn::Error::new_spanned(
                name,
                "range endpoints must be ordered within the same block",
            ));
        }
        edits.add(a, start_point.after, &start)?;
        let final_record = record(&args.record, &span);
        edits.add(
            b,
            end_point.after,
            &quote!(#final_record ::std::mem::drop(#entered); ::std::mem::drop(#span);),
        )?;
    } else {
        for hook in &args.hooks {
            for location in hook.selector.locations(&mut function.block)? {
                edits.add(location, true, &record(&hook.fields, &span))?;
            }
        }
    }
    edits.visit_block_mut(&mut function.block);
    for (i, counter) in args.counters.iter().enumerate() {
        apply_counter(&mut function.block, counter, i, &span)?;
    }
    if args.start.is_none() {
        let body = &function.block;
        function.block = if let Some((binding, fields)) = &args.on_ok {
            let output = match &function.sig.output {
                syn::ReturnType::Type(_, ty) if matches!(ty.as_ref(), syn::Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Result")) => {
                    ty
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &function.sig.output,
                        "on_ok requires a Result return type",
                    ));
                }
            };
            let result = Ident::new("__gluesql_observe_result", Span::mixed_site());
            let writes = record(fields, &span);
            Box::new(parse_quote!({
                #start
                let #result = (|| -> #output #body)();
                if let ::std::result::Result::Ok(#binding) = &#result { #writes }
                #result
            }))
        } else {
            let stmts = &body.stmts;
            Box::new(parse_quote!({ #start #(#stmts)* }))
        };
    }
    Ok(quote!(#function))
}

fn apply_counter(
    block: &mut Block,
    counter: &Counter,
    index: usize,
    span: &Ident,
) -> syn::Result<()> {
    let location = counter.selector.locations(block)?[0];
    let guard = Ident::new(
        &format!("__gluesql_observe_count_{index}"),
        Span::mixed_site(),
    );
    let field = counter.field.to_string();
    let mut editor = CounterBody {
        location,
        block_id: 0,
        counter,
        guard: &guard,
        error: None,
    };
    editor.visit_block_mut(block);
    if let Some(error) = editor.error {
        return Err(error);
    }
    let mut edits = Edits::default();
    edits.add(
        location,
        false,
        &quote! {
            let mut #guard = {
                struct Count { span: tracing::Span, value: u64 }
                impl ::std::ops::Drop for Count {
                    fn drop(&mut self) { self.span.record(#field, self.value); }
                }
                Count { span: #span.clone(), value: 0 }
            };
        },
    )?;
    edits.add(location, true, &quote!(::std::mem::drop(#guard);))?;
    edits.visit_block_mut(block);
    Ok(())
}

struct CounterBody<'a> {
    location: Location,
    block_id: usize,
    counter: &'a Counter,
    guard: &'a Ident,
    error: Option<syn::Error>,
}

impl VisitMut for CounterBody<'_> {
    fn visit_item_mut(&mut self, _: &mut syn::Item) {}
    fn visit_expr_closure_mut(&mut self, _: &mut syn::ExprClosure) {}
    fn visit_expr_async_mut(&mut self, _: &mut syn::ExprAsync) {}
    fn visit_block_mut(&mut self, block: &mut Block) {
        let id = self.block_id;
        self.block_id += 1;
        for (i, stmt) in block.stmts.iter_mut().enumerate() {
            if (Location {
                block: id,
                statement: i,
            }) == self.location
            {
                let Stmt::Expr(Expr::ForLoop(expr), _) = stmt else {
                    unreachable!()
                };
                let guard = self.guard;
                let increment = quote!(if !#guard.span.is_disabled() { #guard.value += 1; });
                if let Some(selector) = &self.counter.increment {
                    let result = selector.locations(&mut expr.body).and_then(|locations| {
                        let mut edits = Edits::default();
                        for location in locations {
                            edits.add(location, true, &increment)?;
                        }
                        edits.visit_block_mut(&mut expr.body);
                        Ok(())
                    });
                    self.error = result.err();
                } else {
                    expr.body.stmts.insert(0, parse_quote!(#increment));
                }
                return;
            }
            self.visit_stmt_mut(stmt);
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_invalid_selectors_and_options() {
        let body = quote!(
            fn example() -> Result<(), ()> {
                let rows = vec![1];
                let rows = rows;
                Ok(())
            }
        );
        for attr in [
            quote!(name = "x", after_let(missing, record(n = 1))),
            quote!(name = "x", after_let(rows, record(n = 1))),
            quote!(name = "x", after_let(rows, occurrence = 0, record(n = 1))),
            quote!(name = "x", after_let(rows, occurrence = 3, record(n = 1))),
            quote!(
                name = "x",
                start = before_let(rows, occurrence = 2),
                end = after_let(rows, occurrence = 1)
            ),
            quote!(name = "x", start = before_let(rows, occurrence = 1)),
            quote!(name = "x", level = "verbose"),
            quote!(name = "x", name = "y"),
            quote!(name = "x", unknown = true),
        ] {
            assert!(
                expand(attr.clone(), body.clone()).is_err(),
                "accepted {attr}"
            );
        }
        for body in [
            quote!(
                async fn example() {}
            ),
            quote!(
                const fn example() {}
            ),
        ] {
            assert!(expand(quote!(name = "x"), body).is_err());
        }
        assert!(
            expand(
                quote!(name = "x", on_ok(v, record(n = v))),
                quote!(
                    fn f() -> usize {
                        1
                    }
                )
            )
            .is_err()
        );
    }

    #[test]
    fn does_not_search_separate_execution_scopes() {
        for body in [
            quote!(
                fn f() {
                    let closure = || {
                        let rows = vec![1];
                    };
                }
            ),
            quote!(
                fn f() {
                    fn nested() {
                        let rows = vec![1];
                    }
                }
            ),
            quote!(
                fn f() {
                    let future = async {
                        let rows = vec![1];
                    };
                }
            ),
        ] {
            assert!(
                expand(
                    quote!(name = "x", after_let(rows, record(n = rows.len()))),
                    body
                )
                .is_err()
            );
        }
        assert!(
            expand(
                quote!(name = "x", start = before_let(a), end = after_let(b)),
                quote!(
                    fn f() {
                        let a = 1;
                        if true {
                            let b = 2;
                        }
                    }
                )
            )
            .is_err()
        );
    }
}
