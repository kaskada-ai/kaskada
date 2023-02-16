use std::fmt::Display;

use logos::{Lexer, Logos};

use crate::LiteralValue;

/// Lex a string literal. Slices off the surrounding `"`, as the current regex
/// includes those.
fn lex_string_literal<'input>(lex: &mut Lexer<'input, Token<'input>>) -> Option<LiteralValue> {
    let slice = lex.slice();
    let slice = &slice[1..slice.len() - 1];

    let mut result = String::with_capacity(slice.len());
    let mut iter = slice.chars();
    while let Some(next) = iter.next() {
        if next == '\\' {
            // Attempt to populate the escape sequence.
            // TODO: Return specific errors for unsupported escape sequences.
            // TODO: Handle other escapes (unicode / etc.)
            match iter.next() {
                Some('\'') => result.push('\''),
                Some('\"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some(_) => return None,
                None => return None,
            }
        } else {
            result.push(next);
        }
    }

    Some(LiteralValue::String(result))
}

#[derive(Debug, Clone, PartialEq, Eq, Logos)]
pub enum Token<'input> {
    #[token("let")]
    KwLet,
    #[token("and")]
    KwAnd,
    #[token("or")]
    KwOr,
    #[token("in")]
    KwIn,
    #[token("$input")]
    KwInput,
    #[token("as")]
    KwAs,
    #[token("const")]
    KwConst,

    // Lex literals.
    #[regex("[0-9]+([.][0-9]+)?(([ui]8)|([ui]16)|([ufi]32)|([ufi]64))?", |lex| { LiteralValue::Number(lex.slice().to_owned()) })]
    #[regex(r#""([^"]|\\")*""#, |lex| { lex_string_literal(lex) } )]
    #[regex(r#"'([^']|\\')*'"#, |lex| { lex_string_literal(lex) } )]
    #[token("true", |_| LiteralValue::True)]
    #[token("false", |_| LiteralValue::False)]
    #[token("null", |_| LiteralValue::Null)]
    Literal(LiteralValue),

    #[regex("[A-Za-z_][A-Za-z0-9_]*", |lex| lex.slice())]
    Ident(&'input str),

    #[token("+")]
    SymPlus,

    #[token("-")]
    SymMinus,

    #[token("*")]
    SymStar,

    #[token("/")]
    SymSlash,

    #[token("|")]
    SymPipe,

    #[token("=")]
    SymEquals,

    #[token("==")]
    SymDoubleEquals,

    #[token("<>")]
    SymLtGt,
    #[token("!=")]
    SymNeq,

    #[token("<")]
    SymLt,
    #[token(">")]
    SymGt,
    #[token("<=")]
    SymLte,
    #[token(">=")]
    SymGte,

    #[token(".")]
    SymDot,
    #[token(",")]
    SymComma,

    #[token("(")]
    SymLParen,

    #[token(")")]
    SymRParen,

    #[token("{")]
    SymLBrace,

    #[token("}")]
    SymRBrace,

    #[token("[")]
    SymLBrack,

    #[token("]")]
    SymRBrack,

    #[token("!")]
    SymExclamation,

    #[token(":")]
    SymColon,
    #[token("->")]
    SymSingleArrow,

    #[error]
    // Skip whitespace
    #[regex("[ \t\n]+", logos::skip)]
    // Skip comments
    #[regex("#.*", logos::skip)]
    Error,

    Unrecognized(&'input str),
}

impl<'a> Display for Token<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::KwLet => write!(f, "let"),
            Token::KwAnd => write!(f, "and"),
            Token::KwOr => write!(f, "or"),
            Token::KwIn => write!(f, "in"),
            Token::KwInput => write!(f, "$input"),
            Token::KwAs => write!(f, "as"),
            Token::KwConst => write!(f, "const"),
            Token::Literal(literal) => write!(f, "{literal}"),
            Token::Ident(ident) => write!(f, "{ident}"),
            Token::SymPlus => write!(f, "+"),
            Token::SymMinus => write!(f, "-"),
            Token::SymStar => write!(f, "*"),
            Token::SymSlash => write!(f, "/"),
            Token::SymPipe => write!(f, "|"),
            Token::SymEquals => write!(f, "="),
            Token::SymDoubleEquals => write!(f, "=="),
            Token::SymNeq => write!(f, "!="),
            Token::SymLtGt => write!(f, "<>"),
            Token::SymLt => write!(f, "<"),
            Token::SymGt => write!(f, ">"),
            Token::SymLte => write!(f, "<="),
            Token::SymGte => write!(f, ">="),
            Token::SymDot => write!(f, "."),
            Token::SymComma => write!(f, ","),
            Token::SymLParen => write!(f, "("),
            Token::SymRParen => write!(f, ")"),
            Token::SymLBrace => write!(f, "{{"),
            Token::SymRBrace => write!(f, "}}"),
            Token::SymLBrack => write!(f, "["),
            Token::SymRBrack => write!(f, "]"),
            Token::SymExclamation => write!(f, "!"),
            Token::SymColon => write!(f, ":"),
            Token::SymSingleArrow => write!(f, "->"),
            Token::Unrecognized(s) => write!(f, "{s}"),
            Token::Error => write!(f, "ERROR"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token<'_>> {
        Token::lexer(input).collect()
    }

    #[test]
    fn test_lex_identifiers() {
        assert_eq!(
            lex("hello world657 a _source_location"),
            vec![
                Token::Ident("hello"),
                Token::Ident("world657"),
                Token::Ident("a"),
                Token::Ident("_source_location")
            ]
        );

        assert_eq!(lex("a#"), vec![Token::Ident("a")]);
        assert_eq!(lex("$input"), vec![Token::KwInput]);
        assert_eq!(lex("truer"), vec![Token::Ident("truer")]);
    }

    #[test]
    fn test_lex_field_ref() {
        assert_eq!(
            lex("a.foo"),
            vec![Token::Ident("a"), Token::SymDot, Token::Ident("foo")]
        )
    }

    #[test]
    fn test_lex_indexing() {
        assert_eq!(
            lex("a[0 + 1]"),
            vec![
                Token::Ident("a"),
                Token::SymLBrack,
                Token::Literal(LiteralValue::Number("0".to_owned())),
                Token::SymPlus,
                Token::Literal(LiteralValue::Number("1".to_owned())),
                Token::SymRBrack
            ]
        )
    }

    #[test]
    fn test_lex_strings() {
        assert_eq!(
            lex("\"hello\" \"with \\\" quotes'\""),
            vec![
                Token::Literal(LiteralValue::String("hello".to_owned())),
                Token::Literal(LiteralValue::String("with \" quotes'".to_owned())),
            ]
        );

        assert_eq!(
            lex("'hello' 'with \\' quotes\"'"),
            vec![
                Token::Literal(LiteralValue::String("hello".to_owned())),
                Token::Literal(LiteralValue::String("with ' quotes\"".to_owned())),
            ]
        );

        assert_eq!(lex("'\\y'"), vec![Token::Error])
    }

    #[test]
    fn test_lex_numbers() {
        assert_eq!(
            lex("0.0 0 1.0 0.2 3 5 1 0.2"),
            vec![
                Token::Literal(LiteralValue::Number("0.0".to_owned())),
                Token::Literal(LiteralValue::Number("0".to_owned())),
                Token::Literal(LiteralValue::Number("1.0".to_owned())),
                Token::Literal(LiteralValue::Number("0.2".to_owned())),
                Token::Literal(LiteralValue::Number("3".to_owned())),
                Token::Literal(LiteralValue::Number("5".to_owned())),
                Token::Literal(LiteralValue::Number("1".to_owned())),
                Token::Literal(LiteralValue::Number("0.2".to_owned())),
            ]
        );

        assert_eq!(
            lex("-0.0 -0 -1.0 -0.2 -3 -5 -1 -0.2"),
            vec![
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("0.0".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("0".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("1.0".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("0.2".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("3".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("5".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("1".to_owned())),
                Token::SymMinus,
                Token::Literal(LiteralValue::Number("0.2".to_owned())),
            ]
        );
    }
}
