use std::collections::HashMap;

pub(super) struct LinkFenlTypes;

impl tera::Filter for LinkFenlTypes {
    fn filter(
        &self,
        value: &tera::Value,
        _args: &HashMap<String, tera::Value>,
    ) -> tera::Result<tera::Value> {
        use logos::Logos;
        use sparrow_syntax::Token;

        // TODO: We could have a filter that tries to parse the signature and
        // format it. This would allow us to do smarter things like only look
        // for types where we would expect them. But, the signature may be a
        // free-form string (for cases that don't yet support signatures) so
        // this strategy of just looking for tokens that would be recognized as
        // types is a nice middle ground that applies in more cases.
        if let Some(input) = value.as_str() {
            let mut lexer = Token::lexer(input);

            let mut output = String::new();
            let mut following_colon = false;
            output.push_str("<pre>");
            while let Some(token) = lexer.next() {
                match token {
                    Token::KwConst => {
                        output.push_str("const ");
                    }
                    Token::Ident(ident) if following_colon => {
                        if let Some(url) = LINKED_IDENT_URLS.get(ident) {
                            output.push_str("<a href=\"");
                            output.push_str(url);
                            output.push_str("\">");
                            output.push_str(ident);
                            output.push_str("</a>");
                        } else {
                            output.push_str(ident);
                        }
                    }
                    Token::SymColon => {
                        // Add a space between the name and the type.
                        output.push_str(": ");
                    }
                    Token::SymComma => {
                        // Add a space between parameters.
                        output.push_str(", ");
                    }
                    Token::SymSingleArrow => {
                        // Add a space around the arrow for the output type.
                        output.push_str(" -> ");
                    }
                    Token::SymEquals => {
                        // Add a space around the equal sign.
                        output.push_str(" = ");
                    }
                    _ => output.push_str(lexer.slice()),
                }
                following_colon = token == Token::SymColon;
            }
            output.push_str("</pre>");

            Ok(tera::Value::String(output))
        } else {
            Err(tera::Error::msg(format!(
                "Unable to find Fenl types in non-string {value:?}"
            )))
        }
    }
}

#[static_init::dynamic]
static LINKED_IDENT_URLS: HashMap<&'static str, &'static str> = {
    let constraint = "https://docs.kaskada.com/docs/data-model#type-constraints";
    let scalar = "https://docs.kaskada.com/docs/data-model#scalars";
    HashMap::from([
        // Constraints
        ("any", constraint),
        ("key", constraint),
        ("number", constraint),
        ("signed", constraint),
        ("float", constraint),
        ("timedelta", constraint),
        ("ordered", constraint),
        // Windows aren't technically implemented as constraints, but we
        // document them as such since they are non-encodable scalars. This
        // aligns well with the fact that we could treat them as a family
        // of window functions.
        ("window", constraint),
        // Scalar types
        ("bool", scalar),
        ("u8", scalar),
        ("u32", scalar),
        ("u64", scalar),
        ("i8", scalar),
        ("i32", scalar),
        ("i64", scalar),
        ("f32", scalar),
        ("f64", scalar),
        ("string", scalar),
        ("timestamp_s", scalar),
        ("timestamp_ms", scalar),
        ("timestamp_us", scalar),
        ("timestamp_ns", scalar),
        ("duration_s", scalar),
        ("duration_ms", scalar),
        ("duration_us", scalar),
        ("duration_ns", scalar),
        ("interval_days", scalar),
        ("interval_months", scalar),
    ])
};
