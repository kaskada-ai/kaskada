//! Adapt the token lexer produced by Logos to an iterator for use with Lalrpop.

use logos::Logos;

use crate::parser::token::Token;

pub type Spanned<Tok, Loc, Error> = Result<(Loc, Tok, Loc), Error>;

/// Adapts the Logos produced lexer for use with LALRPOP.
pub(crate) struct Lexer<'input> {
    lexer: logos::Lexer<'input, Token<'input>>,
}

impl<'input> Lexer<'input> {
    pub(crate) fn new(input: &'input str) -> Self {
        Self {
            lexer: Token::lexer(input),
        }
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Spanned<Token<'input>, usize, (usize, String, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lexer.next().map(|token| {
            let span = self.lexer.span();
            let token = if token == Token::Error {
                Token::Unrecognized(self.lexer.slice())
            } else {
                token
            };
            Ok((span.start, token, span.end))
        })
    }
}
