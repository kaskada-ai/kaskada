use crate::parser::is_valid_ident;
use crate::syntax::*;

fn test_expr(input: &'static str) -> ExprRef {
    Expr::try_from_str(FeatureSetPart::Internal(input), input).unwrap()
}

#[test]
fn test_is_valid_ident() {
    assert!(is_valid_ident("TableFoo"));
    assert!(is_valid_ident("TableFoo_1"));
    assert!(!is_valid_ident("Table.Foo"));
    assert!(!is_valid_ident("Table Foo"));
}

#[test]
fn test_positive_literal() {
    insta::assert_ron_snapshot!(test_expr("1"), @r###"
    Expr(
      op: Literal(Located(
        value: Number("1"),
        location: Location(
          part: Internal("1"),
          start: 0,
          end: 1,
        ),
      )),
      args: Arguments([]),
    )
    "###);
}

#[test]
fn test_negative_literal() {
    insta::assert_ron_snapshot!(test_expr("1.0"), @r###"
    Expr(
      op: Literal(Located(
        value: Number("1.0"),
        location: Location(
          part: Internal("1.0"),
          start: 0,
          end: 3,
        ),
      )),
      args: Arguments([]),
    )
    "###);
}

#[test]
fn test_negative_float_literla() {
    insta::assert_ron_snapshot!(test_expr("-0.1"), @r###"
    Expr(
      op: Call(Located(
        value: "neg",
        location: Location(
          part: Internal("-0.1"),
          start: 0,
          end: 1,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("0.1"),
              location: Location(
                part: Internal("-0.1"),
                start: 1,
                end: 4,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("-0.1"),
            start: 1,
            end: 4,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_arithmetic() {
    insta::assert_ron_snapshot!(test_expr("1 + 2 * 3"), @r###"
    Expr(
      op: Call(Located(
        value: "add",
        location: Location(
          part: Internal("1 + 2 * 3"),
          start: 2,
          end: 3,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("1"),
              location: Location(
                part: Internal("1 + 2 * 3"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("1 + 2 * 3"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "mul",
              location: Location(
                part: Internal("1 + 2 * 3"),
                start: 6,
                end: 7,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("2"),
                    location: Location(
                      part: Internal("1 + 2 * 3"),
                      start: 4,
                      end: 5,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("1 + 2 * 3"),
                  start: 4,
                  end: 5,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("3"),
                    location: Location(
                      part: Internal("1 + 2 * 3"),
                      start: 8,
                      end: 9,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("1 + 2 * 3"),
                  start: 8,
                  end: 9,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("1 + 2 * 3"),
            start: 4,
            end: 9,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parentheses() {
    insta::assert_ron_snapshot!(test_expr("(1 + 2.0) * 3"), @r###"
    Expr(
      op: Call(Located(
        value: "mul",
        location: Location(
          part: Internal("(1 + 2.0) * 3"),
          start: 10,
          end: 11,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "add",
              location: Location(
                part: Internal("(1 + 2.0) * 3"),
                start: 3,
                end: 4,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("1"),
                    location: Location(
                      part: Internal("(1 + 2.0) * 3"),
                      start: 1,
                      end: 2,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("(1 + 2.0) * 3"),
                  start: 1,
                  end: 2,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("2.0"),
                    location: Location(
                      part: Internal("(1 + 2.0) * 3"),
                      start: 5,
                      end: 8,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("(1 + 2.0) * 3"),
                  start: 5,
                  end: 8,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("(1 + 2.0) * 3"),
            start: 0,
            end: 9,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("3"),
              location: Location(
                part: Internal("(1 + 2.0) * 3"),
                start: 12,
                end: 13,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("(1 + 2.0) * 3"),
            start: 12,
            end: 13,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_prefix_neg_literal() {
    insta::assert_ron_snapshot!(test_expr("-5"), @r###"
    Expr(
      op: Call(Located(
        value: "neg",
        location: Location(
          part: Internal("-5"),
          start: 0,
          end: 1,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("5"),
              location: Location(
                part: Internal("-5"),
                start: 1,
                end: 2,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("-5"),
            start: 1,
            end: 2,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_prefix_neg_ident() {
    insta::assert_ron_snapshot!(test_expr("-a"), @r###"
    Expr(
      op: Call(Located(
        value: "neg",
        location: Location(
          part: Internal("-a"),
          start: 0,
          end: 1,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("-a"),
                start: 1,
                end: 2,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("-a"),
            start: 1,
            end: 2,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_prefix_not_ident() {
    insta::assert_ron_snapshot!(test_expr("!a"), @r###"
    Expr(
      op: Call(Located(
        value: "not",
        location: Location(
          part: Internal("!a"),
          start: 0,
          end: 1,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("!a"),
                start: 1,
                end: 2,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("!a"),
            start: 1,
            end: 2,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_or() {
    insta::assert_ron_snapshot!(test_expr("a or b"), @r###"
    Expr(
      op: Call(Located(
        value: "logical_or",
        location: Location(
          part: Internal("a or b"),
          start: 2,
          end: 4,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a or b"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a or b"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("a or b"),
                start: 5,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a or b"),
            start: 5,
            end: 6,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_and() {
    insta::assert_ron_snapshot!(test_expr("a and b"), @r###"
    Expr(
      op: Call(Located(
        value: "logical_and",
        location: Location(
          part: Internal("a and b"),
          start: 2,
          end: 5,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a and b"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a and b"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("a and b"),
                start: 6,
                end: 7,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a and b"),
            start: 6,
            end: 7,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_prefix_precedence() {
    assert_eq!(test_expr("!a or b"), test_expr("(!a) or b"));
    assert_eq!(test_expr("-5 * 3"), test_expr("(-5) * 3"));
    assert_eq!(test_expr("5 * -3"), test_expr("5 * (-3)"));
    assert_eq!(test_expr("-5 + 3"), test_expr("(-5) + 3"));
    assert_eq!(test_expr("5 + -3"), test_expr("5 + (-3)"));
}

#[test]
fn test_pipe_precedence() {
    // "1 + 2 | 3 * 4" == "(1 + 2) | (3 * 4)"
    // If the pipe is the top, then the arithmetic bound tighter.
    let expr = test_expr("1 + 2 | 3 * 4");
    assert!(matches!(expr.op(), ExprOp::Pipe(_)), "Expected pipe");
}

#[test]
fn test_pipe_associativity() {
    insta::assert_ron_snapshot!(test_expr("1 | 2 | 3"), @r###"
    Expr(
      op: Pipe(Location(
        part: Internal("1 | 2 | 3"),
        start: 2,
        end: 3,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("1"),
              location: Location(
                part: Internal("1 | 2 | 3"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("1 | 2 | 3"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Pipe(Location(
              part: Internal("1 | 2 | 3"),
              start: 6,
              end: 7,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("2"),
                    location: Location(
                      part: Internal("1 | 2 | 3"),
                      start: 4,
                      end: 5,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("1 | 2 | 3"),
                  start: 4,
                  end: 5,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("3"),
                    location: Location(
                      part: Internal("1 | 2 | 3"),
                      start: 8,
                      end: 9,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("1 | 2 | 3"),
                  start: 8,
                  end: 9,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("1 | 2 | 3"),
            start: 4,
            end: 9,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_indexing_one_literal() {
    insta::assert_ron_snapshot!(test_expr("a[0]"), @r###"
    Expr(
      op: Call(Located(
        value: "index",
        location: Location(
          part: Internal("a[0]"),
          start: 1,
          end: 4,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a[0]"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a[0]"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("0"),
              location: Location(
                part: Internal("a[0]"),
                start: 2,
                end: 3,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a[0]"),
            start: 2,
            end: 3,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_indexing_with_expressions() {
    insta::assert_ron_snapshot!(test_expr("a[0][1 + 1]"), @r###"
    Expr(
      op: Call(Located(
        value: "index",
        location: Location(
          part: Internal("a[0][1 + 1]"),
          start: 4,
          end: 11,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "index",
              location: Location(
                part: Internal("a[0][1 + 1]"),
                start: 1,
                end: 4,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "a",
                    location: Location(
                      part: Internal("a[0][1 + 1]"),
                      start: 0,
                      end: 1,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0][1 + 1]"),
                  start: 0,
                  end: 1,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("0"),
                    location: Location(
                      part: Internal("a[0][1 + 1]"),
                      start: 2,
                      end: 3,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0][1 + 1]"),
                  start: 2,
                  end: 3,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a[0][1 + 1]"),
            start: 0,
            end: 4,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "add",
              location: Location(
                part: Internal("a[0][1 + 1]"),
                start: 7,
                end: 8,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("1"),
                    location: Location(
                      part: Internal("a[0][1 + 1]"),
                      start: 5,
                      end: 6,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0][1 + 1]"),
                  start: 5,
                  end: 6,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("1"),
                    location: Location(
                      part: Internal("a[0][1 + 1]"),
                      start: 9,
                      end: 10,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0][1 + 1]"),
                  start: 9,
                  end: 10,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a[0][1 + 1]"),
            start: 5,
            end: 10,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_index_arithmetic() {
    insta::assert_ron_snapshot!(test_expr("a[0 + 1]"), @r###"
    Expr(
      op: Call(Located(
        value: "index",
        location: Location(
          part: Internal("a[0 + 1]"),
          start: 1,
          end: 8,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a[0 + 1]"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a[0 + 1]"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "add",
              location: Location(
                part: Internal("a[0 + 1]"),
                start: 4,
                end: 5,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("0"),
                    location: Location(
                      part: Internal("a[0 + 1]"),
                      start: 2,
                      end: 3,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0 + 1]"),
                  start: 2,
                  end: 3,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("1"),
                    location: Location(
                      part: Internal("a[0 + 1]"),
                      start: 6,
                      end: 7,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a[0 + 1]"),
                  start: 6,
                  end: 7,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a[0 + 1]"),
            start: 2,
            end: 7,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_field_ref() {
    insta::assert_ron_snapshot!(test_expr("a.foo"), @r###"
    Expr(
      op: FieldRef(Located(
        value: "foo",
        location: Location(
          part: Internal("a.foo"),
          start: 2,
          end: 5,
        ),
      ), Location(
        part: Internal("a.foo"),
        start: 1,
        end: 2,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a.foo"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a.foo"),
            start: 0,
            end: 1,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_field_ref_with_indexing() {
    insta::assert_ron_snapshot!(test_expr("a[0].foo[1]"), @r###"
    Expr(
      op: Call(Located(
        value: "index",
        location: Location(
          part: Internal("a[0].foo[1]"),
          start: 8,
          end: 11,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: FieldRef(Located(
              value: "foo",
              location: Location(
                part: Internal("a[0].foo[1]"),
                start: 5,
                end: 8,
              ),
            ), Location(
              part: Internal("a[0].foo[1]"),
              start: 4,
              end: 5,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Call(Located(
                    value: "index",
                    location: Location(
                      part: Internal("a[0].foo[1]"),
                      start: 1,
                      end: 4,
                    ),
                  )),
                  args: Arguments([
                    Positional(Located(
                      value: Expr(
                        op: Reference(Located(
                          value: "a",
                          location: Location(
                            part: Internal("a[0].foo[1]"),
                            start: 0,
                            end: 1,
                          ),
                        )),
                        args: Arguments([]),
                      ),
                      location: Location(
                        part: Internal("a[0].foo[1]"),
                        start: 0,
                        end: 1,
                      ),
                    )),
                    Positional(Located(
                      value: Expr(
                        op: Literal(Located(
                          value: Number("0"),
                          location: Location(
                            part: Internal("a[0].foo[1]"),
                            start: 2,
                            end: 3,
                          ),
                        )),
                        args: Arguments([]),
                      ),
                      location: Location(
                        part: Internal("a[0].foo[1]"),
                        start: 2,
                        end: 3,
                      ),
                    )),
                  ]),
                ),
                location: Location(
                  part: Internal("a[0].foo[1]"),
                  start: 0,
                  end: 4,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a[0].foo[1]"),
            start: 0,
            end: 8,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("1"),
              location: Location(
                part: Internal("a[0].foo[1]"),
                start: 9,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a[0].foo[1]"),
            start: 9,
            end: 10,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_field_ref_precedence() {
    // "foo.x + bar.y" == "(foo.x) + (bar.y)"
    let expr = test_expr("foo.x + bar.y");

    // If the plus is the top, then the field refs bound tighter.
    assert!(matches!(expr.op(), ExprOp::Call(location) if location.inner() == "add"));
}

#[test]
fn test_parse_call() {
    insta::assert_ron_snapshot!(test_expr("sum(Sent.amount)"), @r###"
    Expr(
      op: Call(Located(
        value: "sum",
        location: Location(
          part: Internal("sum(Sent.amount)"),
          start: 0,
          end: 3,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: FieldRef(Located(
              value: "amount",
              location: Location(
                part: Internal("sum(Sent.amount)"),
                start: 9,
                end: 15,
              ),
            ), Location(
              part: Internal("sum(Sent.amount)"),
              start: 8,
              end: 9,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "Sent",
                    location: Location(
                      part: Internal("sum(Sent.amount)"),
                      start: 4,
                      end: 8,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("sum(Sent.amount)"),
                  start: 4,
                  end: 8,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("sum(Sent.amount)"),
            start: 4,
            end: 15,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_call_with_expressions() {
    insta::assert_ron_snapshot!(test_expr("sum(Sent.amount + 1, 2) + 3"), @r###"
    Expr(
      op: Call(Located(
        value: "add",
        location: Location(
          part: Internal("sum(Sent.amount + 1, 2) + 3"),
          start: 24,
          end: 25,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "sum",
              location: Location(
                part: Internal("sum(Sent.amount + 1, 2) + 3"),
                start: 0,
                end: 3,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Call(Located(
                    value: "add",
                    location: Location(
                      part: Internal("sum(Sent.amount + 1, 2) + 3"),
                      start: 16,
                      end: 17,
                    ),
                  )),
                  args: Arguments([
                    Positional(Located(
                      value: Expr(
                        op: FieldRef(Located(
                          value: "amount",
                          location: Location(
                            part: Internal("sum(Sent.amount + 1, 2) + 3"),
                            start: 9,
                            end: 15,
                          ),
                        ), Location(
                          part: Internal("sum(Sent.amount + 1, 2) + 3"),
                          start: 8,
                          end: 9,
                        )),
                        args: Arguments([
                          Positional(Located(
                            value: Expr(
                              op: Reference(Located(
                                value: "Sent",
                                location: Location(
                                  part: Internal("sum(Sent.amount + 1, 2) + 3"),
                                  start: 4,
                                  end: 8,
                                ),
                              )),
                              args: Arguments([]),
                            ),
                            location: Location(
                              part: Internal("sum(Sent.amount + 1, 2) + 3"),
                              start: 4,
                              end: 8,
                            ),
                          )),
                        ]),
                      ),
                      location: Location(
                        part: Internal("sum(Sent.amount + 1, 2) + 3"),
                        start: 4,
                        end: 15,
                      ),
                    )),
                    Positional(Located(
                      value: Expr(
                        op: Literal(Located(
                          value: Number("1"),
                          location: Location(
                            part: Internal("sum(Sent.amount + 1, 2) + 3"),
                            start: 18,
                            end: 19,
                          ),
                        )),
                        args: Arguments([]),
                      ),
                      location: Location(
                        part: Internal("sum(Sent.amount + 1, 2) + 3"),
                        start: 18,
                        end: 19,
                      ),
                    )),
                  ]),
                ),
                location: Location(
                  part: Internal("sum(Sent.amount + 1, 2) + 3"),
                  start: 4,
                  end: 19,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("2"),
                    location: Location(
                      part: Internal("sum(Sent.amount + 1, 2) + 3"),
                      start: 21,
                      end: 22,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("sum(Sent.amount + 1, 2) + 3"),
                  start: 21,
                  end: 22,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("sum(Sent.amount + 1, 2) + 3"),
            start: 0,
            end: 23,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("3"),
              location: Location(
                part: Internal("sum(Sent.amount + 1, 2) + 3"),
                start: 26,
                end: 27,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("sum(Sent.amount + 1, 2) + 3"),
            start: 26,
            end: 27,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_let() {
    insta::assert_ron_snapshot!(test_expr("let a = 5 in a + 1"), @r###"
    Expr(
      op: Let([
        Located(
          value: "a",
          location: Location(
            part: Internal("let a = 5 in a + 1"),
            start: 4,
            end: 5,
          ),
        ),
        Located(
          value: "let_body",
          location: Location(
            part: Internal("let_body"),
            start: 0,
            end: 8,
          ),
        ),
      ], Location(
        part: Internal("let a = 5 in a + 1"),
        start: 0,
        end: 18,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("5"),
              location: Location(
                part: Internal("let a = 5 in a + 1"),
                start: 8,
                end: 9,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("let a = 5 in a + 1"),
            start: 8,
            end: 9,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "add",
              location: Location(
                part: Internal("let a = 5 in a + 1"),
                start: 15,
                end: 16,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "a",
                    location: Location(
                      part: Internal("let a = 5 in a + 1"),
                      start: 13,
                      end: 14,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("let a = 5 in a + 1"),
                  start: 13,
                  end: 14,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Literal(Located(
                    value: Number("1"),
                    location: Location(
                      part: Internal("let a = 5 in a + 1"),
                      start: 17,
                      end: 18,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("let a = 5 in a + 1"),
                  start: 17,
                  end: 18,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("let a = 5 in a + 1"),
            start: 13,
            end: 18,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_record() {
    insta::assert_ron_snapshot!(test_expr("{ a: 5, b: 6, c: d, e, f: 8}"), @r###"
    Expr(
      op: Record([
        Located(
          value: "a",
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 2,
            end: 3,
          ),
        ),
        Located(
          value: "b",
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 8,
            end: 9,
          ),
        ),
        Located(
          value: "c",
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 14,
            end: 15,
          ),
        ),
        Located(
          value: "e",
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 20,
            end: 21,
          ),
        ),
        Located(
          value: "f",
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 23,
            end: 24,
          ),
        ),
      ], Location(
        part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
        start: 0,
        end: 28,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("5"),
              location: Location(
                part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
                start: 5,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 5,
            end: 6,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("6"),
              location: Location(
                part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
                start: 11,
                end: 12,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 11,
            end: 12,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "d",
              location: Location(
                part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
                start: 17,
                end: 18,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 17,
            end: 18,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "e",
              location: Location(
                part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
                start: 20,
                end: 21,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 20,
            end: 21,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Literal(Located(
              value: Number("8"),
              location: Location(
                part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
                start: 26,
                end: 27,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("{ a: 5, b: 6, c: d, e, f: 8}"),
            start: 26,
            end: 27,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_extend_record_as_call() {
    insta::assert_ron_snapshot!(test_expr("extend(w, b)"), @r###"
    Expr(
      op: ExtendRecord(Location(
        part: Internal("extend(w, b)"),
        start: 0,
        end: 6,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "w",
              location: Location(
                part: Internal("extend(w, b)"),
                start: 7,
                end: 8,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("extend(w, b)"),
            start: 7,
            end: 8,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("extend(w, b)"),
                start: 10,
                end: 11,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("extend(w, b)"),
            start: 10,
            end: 11,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_extend_record_with_pipe() {
    insta::assert_ron_snapshot!(test_expr("b | extend(w)"), @r###"
    Expr(
      op: Pipe(Location(
        part: Internal("b | extend(w)"),
        start: 2,
        end: 3,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("b | extend(w)"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("b | extend(w)"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: ExtendRecord(Location(
              part: Internal("b | extend(w)"),
              start: 4,
              end: 10,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "w",
                    location: Location(
                      part: Internal("b | extend(w)"),
                      start: 11,
                      end: 12,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("b | extend(w)"),
                  start: 11,
                  end: 12,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("b | extend(w)"),
            start: 4,
            end: 13,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_extend_record_multiple_with_pipe() {
    insta::assert_ron_snapshot!(test_expr("a | extend(b) | extend(c)"), @r###"
    Expr(
      op: Pipe(Location(
        part: Internal("a | extend(b) | extend(c)"),
        start: 2,
        end: 3,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a | extend(b) | extend(c)"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a | extend(b) | extend(c)"),
            start: 0,
            end: 1,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Pipe(Location(
              part: Internal("a | extend(b) | extend(c)"),
              start: 14,
              end: 15,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: ExtendRecord(Location(
                    part: Internal("a | extend(b) | extend(c)"),
                    start: 4,
                    end: 10,
                  )),
                  args: Arguments([
                    Positional(Located(
                      value: Expr(
                        op: Reference(Located(
                          value: "b",
                          location: Location(
                            part: Internal("a | extend(b) | extend(c)"),
                            start: 11,
                            end: 12,
                          ),
                        )),
                        args: Arguments([]),
                      ),
                      location: Location(
                        part: Internal("a | extend(b) | extend(c)"),
                        start: 11,
                        end: 12,
                      ),
                    )),
                  ]),
                ),
                location: Location(
                  part: Internal("a | extend(b) | extend(c)"),
                  start: 4,
                  end: 13,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: ExtendRecord(Location(
                    part: Internal("a | extend(b) | extend(c)"),
                    start: 16,
                    end: 22,
                  )),
                  args: Arguments([
                    Positional(Located(
                      value: Expr(
                        op: Reference(Located(
                          value: "c",
                          location: Location(
                            part: Internal("a | extend(b) | extend(c)"),
                            start: 23,
                            end: 24,
                          ),
                        )),
                        args: Arguments([]),
                      ),
                      location: Location(
                        part: Internal("a | extend(b) | extend(c)"),
                        start: 23,
                        end: 24,
                      ),
                    )),
                  ]),
                ),
                location: Location(
                  part: Internal("a | extend(b) | extend(c)"),
                  start: 16,
                  end: 25,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a | extend(b) | extend(c)"),
            start: 4,
            end: 25,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_cast() {
    insta::assert_ron_snapshot!(test_expr("a as i32"), @r###"
    Expr(
      op: Cast(Located(
        value: Concrete(Int32),
        location: Location(
          part: Internal("a as i32"),
          start: 5,
          end: 8,
        ),
      ), Location(
        part: Internal("a as i32"),
        start: 2,
        end: 4,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a as i32"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          location: Location(
            part: Internal("a as i32"),
            start: 0,
            end: 1,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_cast_with_or() {
    insta::assert_ron_snapshot!(test_expr("a or b as i32"), @r###"
    Expr(
      op: Cast(Located(
        value: Concrete(Int32),
        location: Location(
          part: Internal("a or b as i32"),
          start: 10,
          end: 13,
        ),
      ), Location(
        part: Internal("a or b as i32"),
        start: 7,
        end: 9,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Call(Located(
              value: "logical_or",
              location: Location(
                part: Internal("a or b as i32"),
                start: 2,
                end: 4,
              ),
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "a",
                    location: Location(
                      part: Internal("a or b as i32"),
                      start: 0,
                      end: 1,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a or b as i32"),
                  start: 0,
                  end: 1,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "b",
                    location: Location(
                      part: Internal("a or b as i32"),
                      start: 5,
                      end: 6,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a or b as i32"),
                  start: 5,
                  end: 6,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a or b as i32"),
            start: 0,
            end: 6,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_cast_with_comparison() {
    insta::assert_ron_snapshot!(test_expr("(a as i32) < (b as i32)"), @r###"
    Expr(
      op: Call(Located(
        value: "lt",
        location: Location(
          part: Internal("(a as i32) < (b as i32)"),
          start: 11,
          end: 12,
        ),
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Cast(Located(
              value: Concrete(Int32),
              location: Location(
                part: Internal("(a as i32) < (b as i32)"),
                start: 6,
                end: 9,
              ),
            ), Location(
              part: Internal("(a as i32) < (b as i32)"),
              start: 3,
              end: 5,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "a",
                    location: Location(
                      part: Internal("(a as i32) < (b as i32)"),
                      start: 1,
                      end: 2,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("(a as i32) < (b as i32)"),
                  start: 1,
                  end: 2,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("(a as i32) < (b as i32)"),
            start: 0,
            end: 10,
          ),
        )),
        Positional(Located(
          value: Expr(
            op: Cast(Located(
              value: Concrete(Int32),
              location: Location(
                part: Internal("(a as i32) < (b as i32)"),
                start: 19,
                end: 22,
              ),
            ), Location(
              part: Internal("(a as i32) < (b as i32)"),
              start: 16,
              end: 18,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "b",
                    location: Location(
                      part: Internal("(a as i32) < (b as i32)"),
                      start: 14,
                      end: 15,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("(a as i32) < (b as i32)"),
                  start: 14,
                  end: 15,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("(a as i32) < (b as i32)"),
            start: 13,
            end: 23,
          ),
        )),
      ]),
    )
    "###);
}

#[test]
fn test_parse_cast_with_pipe() {
    insta::assert_ron_snapshot!(test_expr("a | b as i32"), @r###"
    Expr(
      op: Cast(Located(
        value: Concrete(Int32),
        location: Location(
          part: Internal("a | b as i32"),
          start: 9,
          end: 12,
        ),
      ), Location(
        part: Internal("a | b as i32"),
        start: 6,
        end: 8,
      )),
      args: Arguments([
        Positional(Located(
          value: Expr(
            op: Pipe(Location(
              part: Internal("a | b as i32"),
              start: 2,
              end: 3,
            )),
            args: Arguments([
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "a",
                    location: Location(
                      part: Internal("a | b as i32"),
                      start: 0,
                      end: 1,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a | b as i32"),
                  start: 0,
                  end: 1,
                ),
              )),
              Positional(Located(
                value: Expr(
                  op: Reference(Located(
                    value: "b",
                    location: Location(
                      part: Internal("a | b as i32"),
                      start: 4,
                      end: 5,
                    ),
                  )),
                  args: Arguments([]),
                ),
                location: Location(
                  part: Internal("a | b as i32"),
                  start: 4,
                  end: 5,
                ),
              )),
            ]),
          ),
          location: Location(
            part: Internal("a | b as i32"),
            start: 0,
            end: 5,
          ),
        )),
      ]),
    )
    "###);
}
