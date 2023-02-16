use std::collections::HashMap;

pub(super) struct CsvToMdFilter;

impl tera::Filter for CsvToMdFilter {
    fn filter(
        &self,
        value: &tera::Value,
        _args: &HashMap<String, tera::Value>,
    ) -> tera::Result<tera::Value> {
        use prettytable::format::TableFormat;
        use prettytable::{Cell, Row, Table};

        if let Some(csv) = value.as_str() {
            let mut csv = Table::from_csv_string(csv)
                .map_err(|e| tera::Error::msg(format!("Failed to parse CSV: {e}")))?;
            let mut format = TableFormat::new();
            format.column_separator('|');
            format.left_border('|');
            format.right_border('|');

            csv.set_format(format);

            let num_columns = csv.get_row(0).unwrap().len();
            let header_cells = vec![Cell::new(" :---: "); num_columns];
            csv.insert_row(1, Row::new(header_cells));
            let mut md = Vec::new();
            csv.print(&mut md)
                .map_err(|e| tera::Error::msg(format!("Failed to write markdown table: {e}")))?;
            let md = String::from_utf8(md)
                .map_err(|e| tera::Error::msg(format!("Failed to convert table to string: {e}")))?;
            Ok(tera::Value::String(md))
        } else {
            Err(tera::Error::msg(format!(
                "Unable to parse CSV from non-string {value:?}",
            )))
        }
    }
}

/// Wraps a string with the necessary formatting to be a warning block.
pub(super) struct WarningBlockQuote;

impl tera::Filter for WarningBlockQuote {
    fn filter(
        &self,
        value: &tera::Value,
        _args: &HashMap<String, tera::Value>,
    ) -> tera::Result<tera::Value> {
        if let Some(content) = value.as_str() {
            // Can grow, but allocate enough space for at least a few lines.
            let mut result = String::with_capacity(content.len() + 64);
            result.push_str("> 🚧 Warning\n");
            for line in content.lines() {
                result.push_str("> ");
                result.push_str(line);
                result.push('\n');
            }
            Ok(tera::Value::String(result))
        } else {
            Err(tera::Error::msg(format!(
                "Unable to format non-string as warning-block: {value:?}"
            )))
        }
    }
}

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
