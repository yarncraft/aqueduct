//! The route expression language (ADR 0002 §3): a closed, typed, three-valued predicate over one
//! change event, and `{...}` templates over the same field paths.
//!
//! Rows are `RawValue` JSON. They are parsed lazily and borrowed: a top-level
//! `HashMap<&str, &RawValue>` is built the first time a path reads a row, and nested objects are
//! parsed only for dotted paths. No `serde_json::Value` tree is built.
//!
//! Rules that matter (see the ADR for the full text):
//! - A missing field is `null`. A comparison with `null` is *unknown*; `where` ending unknown filters.
//! - Comparison is typed by the **literal**. A number literal compares exactly (decimal, not `f64`)
//!   against a JSON number or a string that parses as a decimal. `NaN`/`±Infinity` are type errors.
//!   A string literal allows only `==`, `!=`, `in` (ordering is rejected at parse time).
//! - A type error wins over true, false and unknown, with no short-circuit, so `not (...)` can never
//!   turn a mis-typed row into a pass.
//! - Parsing is capped at [`MAX_EXPR_BYTES`] of input and [`MAX_DEPTH`] nesting levels.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use aqueduct_core::{ChangeEvent, Op};
use serde::Deserialize;
use serde_json::value::RawValue;

/// Longest accepted expression or template, in bytes.
pub const MAX_EXPR_BYTES: usize = 4096;
/// Deepest accepted nesting of parentheses and `not`.
pub const MAX_DEPTH: usize = 32;
/// Digits and exponent bounds for decimals, so a hostile value cannot make comparison expensive.
const MAX_DIGITS: usize = 4096;
const MAX_EXP: i64 = 100_000;

// ---------------------------------------------------------------------------------------------
// Three-valued logic
// ---------------------------------------------------------------------------------------------

/// SQL-style truth value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    fn from_bool(b: bool) -> Self {
        if b {
            Tri::True
        } else {
            Tri::False
        }
    }
    fn not(self) -> Self {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
    fn and(self, o: Self) -> Self {
        match (self, o) {
            (Tri::False, _) | (_, Tri::False) => Tri::False,
            (Tri::True, Tri::True) => Tri::True,
            _ => Tri::Unknown,
        }
    }
    fn or(self, o: Self) -> Self {
        match (self, o) {
            (Tri::True, _) | (_, Tri::True) => Tri::True,
            (Tri::False, Tri::False) => Tri::False,
            _ => Tri::Unknown,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

/// A syntax or validation error found at startup. `pos` is a byte offset into the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub pos: usize,
    pub msg: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (at byte {})", self.msg, self.pos)
    }
}

impl std::error::Error for ParseError {}

fn perr<T>(pos: usize, msg: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError {
        pos,
        msg: msg.into(),
    })
}

/// A row-level type error. It names the path and the expected type, never the value, because the
/// text ends up in the `aqueduct-error` header and in logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalError {
    pub path: String,
    pub expected: &'static str,
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "type error: `{}` is not {}", self.path, self.expected)
    }
}

impl std::error::Error for EvalError {}

// ---------------------------------------------------------------------------------------------
// Field paths
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Op,
    Schema,
    Table,
    Before,
    After,
    /// `after`, else `before`.
    Row,
}

/// A reference to one value of the event: `op`, `schema`, `table`, or `before.`/`after.`/`row.`
/// followed by a column name or a dotted path into a nested document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldRef {
    source: Source,
    path: Vec<String>,
    text: String,
}

impl FieldRef {
    /// Parses `op`, `schema`, `table`, `before.<path>`, `after.<path>` or `row.<path>`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut parts = text.split('.');
        let head = parts.next().unwrap_or_default();
        let rest: Vec<String> = parts.map(str::to_string).collect();
        if rest.iter().any(String::is_empty) || text.ends_with('.') {
            return Err(format!("empty path segment in `{text}`"));
        }
        let source = match (head, rest.is_empty()) {
            ("op", true) => Source::Op,
            ("schema", true) => Source::Schema,
            ("table", true) => Source::Table,
            ("op" | "schema" | "table", false) => {
                return Err(format!("`{head}` has no sub-fields (in `{text}`)"))
            }
            ("before" | "after" | "row", true) => {
                return Err(format!("`{head}` needs a column: `{head}.<column>`"))
            }
            ("before", false) => Source::Before,
            ("after", false) => Source::After,
            ("row", false) => Source::Row,
            _ => {
                return Err(format!(
                    "unknown field `{text}`: use op, schema, table, before.<col>, after.<col> or row.<col>"
                ))
            }
        };
        Ok(Self {
            source,
            path: rest,
            text: text.to_string(),
        })
    }

    /// The path as written, e.g. `row.address.city`.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The first column name below `before`/`after`/`row`, if the field reads a row.
    pub fn column(&self) -> Option<&str> {
        match self.source {
            Source::Before | Source::After | Source::Row => self.path.first().map(String::as_str),
            _ => None,
        }
    }
}

impl fmt::Display for FieldRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

// ---------------------------------------------------------------------------------------------
// Borrowed JSON access
// ---------------------------------------------------------------------------------------------

/// Object key that borrows from the input when it has no escapes.
#[derive(Deserialize, PartialEq, Eq, Hash)]
struct Key<'a>(#[serde(borrow)] Cow<'a, str>);

/// String value that borrows from the input when it has no escapes.
#[derive(Deserialize)]
struct Str<'a>(#[serde(borrow)] Cow<'a, str>);

type Object<'a> = HashMap<Key<'a>, &'a RawValue>;

/// What a field holds, classified by the first byte of its JSON text.
#[derive(Debug, PartialEq)]
enum Leaf<'a> {
    Null,
    Bool(bool),
    /// The number as written.
    Num(&'a str),
    Str(Cow<'a, str>),
    /// An object or array.
    Other,
}

fn leaf(raw: &RawValue) -> Leaf<'_> {
    let t = raw.get().trim_start();
    match t.as_bytes().first() {
        Some(b'n') => Leaf::Null,
        Some(b't') => Leaf::Bool(true),
        Some(b'f') => Leaf::Bool(false),
        Some(b'"') => match serde_json::from_str::<Str>(t) {
            Ok(Str(s)) => Leaf::Str(s),
            Err(_) => Leaf::Other,
        },
        Some(b'{' | b'[') => Leaf::Other,
        Some(_) => Leaf::Num(t.trim_end()),
        None => Leaf::Null,
    }
}

/// An event with its rows parsed on first use. Build one per event, evaluate any number of
/// expressions and templates against it.
pub struct EventView<'a> {
    ev: &'a ChangeEvent,
    before: OnceCell<Option<Object<'a>>>,
    after: OnceCell<Option<Object<'a>>>,
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
        Op::Truncate => "truncate",
        Op::Heartbeat => "heartbeat",
    }
}

impl<'a> EventView<'a> {
    pub fn new(ev: &'a ChangeEvent) -> Self {
        Self {
            ev,
            before: OnceCell::new(),
            after: OnceCell::new(),
        }
    }

    pub fn event(&self) -> &'a ChangeEvent {
        self.ev
    }

    fn top(&self, after: bool) -> Option<&Object<'a>> {
        let (cell, raw) = if after {
            (&self.after, self.ev.after.as_deref())
        } else {
            (&self.before, self.ev.before.as_deref())
        };
        cell.get_or_init(|| raw.and_then(|r| serde_json::from_str::<Object>(r.get()).ok()))
            .as_ref()
    }

    /// The raw JSON of a row field, `None` when absent.
    fn lookup(&self, f: &FieldRef) -> Option<&'a RawValue> {
        let top = match f.source {
            Source::Before => self.top(false),
            Source::After => self.top(true),
            Source::Row => {
                if self.ev.after.is_some() {
                    self.top(true)
                } else {
                    self.top(false)
                }
            }
            _ => return None,
        }?;
        let mut path = f.path.iter();
        let mut cur: &'a RawValue = top.get(&Key(Cow::Borrowed(path.next()?.as_str())))?;
        for seg in path {
            let nested: Object<'a> = serde_json::from_str(cur.get()).ok()?;
            cur = nested.get(&Key(Cow::Borrowed(seg.as_str())))?;
        }
        Some(cur)
    }

    /// True when the row has the field, even if its value is `null`. Used to count rows that lack a
    /// configured column (ADR 0002 §5, unseen fields).
    pub fn present(&self, f: &FieldRef) -> bool {
        match f.source {
            Source::Op | Source::Schema | Source::Table => true,
            _ => self.lookup(f).is_some(),
        }
    }

    fn value(&self, f: &FieldRef) -> Leaf<'a> {
        match f.source {
            Source::Op => Leaf::Str(Cow::Borrowed(op_name(self.ev.op))),
            Source::Schema => Leaf::Str(Cow::Borrowed(self.ev.schema.as_str())),
            Source::Table => Leaf::Str(Cow::Borrowed(self.ev.table.as_str())),
            _ => self.lookup(f).map_or(Leaf::Null, leaf),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Exact decimals
// ---------------------------------------------------------------------------------------------

/// A decimal for exact comparison: `0.<mant> * 10^pow`, `mant` without leading or trailing zeros.
/// Zero has an empty `mant`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dec {
    neg: bool,
    mant: Vec<u8>,
    pow: i64,
}

impl Dec {
    /// Accepts `[+-]digits[.digits][e[+-]digits]` and `[+-].digits`. Rejects everything else,
    /// including `NaN`, `Infinity` and an empty string.
    fn parse(s: &str) -> Option<Dec> {
        let b = s.as_bytes();
        let mut i = 0;
        let neg = match b.first() {
            Some(b'-') => {
                i = 1;
                true
            }
            Some(b'+') => {
                i = 1;
                false
            }
            _ => false,
        };
        let int_start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        let int_digits = &b[int_start..i];
        let mut frac_digits: &[u8] = &[];
        if b.get(i) == Some(&b'.') {
            i += 1;
            let f = i;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            frac_digits = &b[f..i];
        }
        if int_digits.is_empty() && frac_digits.is_empty() {
            return None;
        }
        let mut exp: i64 = 0;
        if matches!(b.get(i), Some(b'e' | b'E')) {
            i += 1;
            let eneg = match b.get(i) {
                Some(b'-') => {
                    i += 1;
                    true
                }
                Some(b'+') => {
                    i += 1;
                    false
                }
                _ => false,
            };
            let e = i;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            if e == i || i - e > 7 {
                return None;
            }
            exp = s.get(e..i)?.parse().ok()?;
            if eneg {
                exp = -exp;
            }
        }
        if i != b.len() || int_digits.len() + frac_digits.len() > MAX_DIGITS || exp.abs() > MAX_EXP
        {
            return None;
        }
        let mut pow = int_digits.len() as i64 + exp;
        let mut mant: Vec<u8> = int_digits.iter().chain(frac_digits).copied().collect();
        let lead = mant.iter().take_while(|&&d| d == b'0').count();
        mant.drain(..lead);
        pow -= lead as i64;
        while mant.last() == Some(&b'0') {
            mant.pop();
        }
        if mant.is_empty() {
            return Some(Dec {
                neg: false,
                mant,
                pow: 0,
            });
        }
        Some(Dec { neg, mant, pow })
    }

    fn magnitude_cmp(&self, o: &Dec) -> Ordering {
        self.pow.cmp(&o.pow).then_with(|| self.mant.cmp(&o.mant))
    }

    fn cmp(&self, o: &Dec) -> Ordering {
        match (self.mant.is_empty(), o.mant.is_empty()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if o.neg {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                return if self.neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            _ => {}
        }
        match (self.neg, o.neg) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.magnitude_cmp(o),
            (true, true) => o.magnitude_cmp(self),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn holds(self, o: Ordering) -> bool {
        match self {
            Cmp::Eq => o == Ordering::Equal,
            Cmp::Ne => o != Ordering::Equal,
            Cmp::Lt => o == Ordering::Less,
            Cmp::Le => o != Ordering::Greater,
            Cmp::Gt => o == Ordering::Greater,
            Cmp::Ge => o != Ordering::Less,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Lit {
    Str(String),
    Num(Dec),
    Bool(bool),
}

impl Lit {
    fn kind(&self) -> &'static str {
        match self {
            Lit::Str(_) => "string",
            Lit::Num(_) => "number",
            Lit::Bool(_) => "boolean",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Or(Vec<Node>),
    And(Vec<Node>),
    Not(Box<Node>),
    Cmp(FieldRef, Cmp, Lit),
    In(FieldRef, Vec<Lit>),
    IsNull(FieldRef, bool),
}

/// A compiled predicate. Build once at startup with [`Expr::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expr {
    root: Node,
    source: String,
}

impl Expr {
    pub fn parse(input: &str) -> Result<Expr, ParseError> {
        if input.len() > MAX_EXPR_BYTES {
            return perr(
                0,
                format!("expression is longer than {MAX_EXPR_BYTES} bytes"),
            );
        }
        let toks = lex(input)?;
        let mut p = Parser {
            toks: &toks,
            i: 0,
            depth: 0,
            end: input.len(),
        };
        let root = p.or()?;
        if let Some(t) = p.toks.get(p.i) {
            return perr(t.pos, "unexpected input after the expression");
        }
        Ok(Expr {
            root,
            source: input.to_string(),
        })
    }

    /// The expression text as configured.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Every field path the expression reads, for the unseen-field check and for the rule that no
    /// masked, hashed or excluded column may be read by routing.
    pub fn fields(&self) -> Vec<&FieldRef> {
        let mut out = Vec::new();
        collect_fields(&self.root, &mut out);
        out
    }

    /// Evaluates against an event. `Err` is a row-level type error and follows the error policy.
    pub fn eval(&self, view: &EventView<'_>) -> Result<Tri, EvalError> {
        eval(&self.root, view)
    }

    /// True only when the predicate is definitely true: an unknown result is filtered.
    pub fn matches(&self, view: &EventView<'_>) -> Result<bool, EvalError> {
        Ok(self.eval(view)? == Tri::True)
    }
}

fn collect_fields<'a>(n: &'a Node, out: &mut Vec<&'a FieldRef>) {
    match n {
        Node::Or(v) | Node::And(v) => v.iter().for_each(|c| collect_fields(c, out)),
        Node::Not(c) => collect_fields(c, out),
        Node::Cmp(f, ..) | Node::In(f, _) | Node::IsNull(f, _) => out.push(f),
    }
}

// ---------------------------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------------------------

fn eval(n: &Node, view: &EventView<'_>) -> Result<Tri, EvalError> {
    match n {
        // Every operand is evaluated, with no short-circuit, so a type error anywhere wins.
        Node::Or(v) => fold(v, view, Tri::False, Tri::or),
        Node::And(v) => fold(v, view, Tri::True, Tri::and),
        Node::Not(c) => Ok(eval(c, view)?.not()),
        Node::IsNull(f, negated) => {
            let null = matches!(view.value(f), Leaf::Null);
            Ok(Tri::from_bool(null != *negated))
        }
        Node::Cmp(f, op, lit) => compare(f, *op, lit, &view.value(f)),
        Node::In(f, lits) => {
            let v = view.value(f);
            let mut acc = Tri::False;
            let mut err = None;
            for lit in lits {
                match compare(f, Cmp::Eq, lit, &v) {
                    Ok(t) => acc = acc.or(t),
                    Err(e) => {
                        err.get_or_insert(e);
                    }
                }
            }
            match err {
                Some(e) => Err(e),
                None => Ok(acc),
            }
        }
    }
}

fn fold(
    v: &[Node],
    view: &EventView<'_>,
    init: Tri,
    f: fn(Tri, Tri) -> Tri,
) -> Result<Tri, EvalError> {
    let mut acc = init;
    let mut err = None;
    for c in v {
        match eval(c, view) {
            Ok(t) => acc = f(acc, t),
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e),
        None => Ok(acc),
    }
}

fn compare(f: &FieldRef, op: Cmp, lit: &Lit, v: &Leaf<'_>) -> Result<Tri, EvalError> {
    let bad = |expected| {
        Err(EvalError {
            path: f.text.clone(),
            expected,
        })
    };
    if matches!(v, Leaf::Null) {
        return Ok(Tri::Unknown);
    }
    match lit {
        Lit::Num(want) => {
            let have = match v {
                Leaf::Num(t) => Dec::parse(t),
                Leaf::Str(s) => Dec::parse(s),
                _ => None,
            };
            match have {
                Some(h) => Ok(Tri::from_bool(op.holds(h.cmp(want)))),
                None => bad("a finite number"),
            }
        }
        Lit::Str(want) => match v {
            Leaf::Str(s) => Ok(Tri::from_bool(op.holds(s.as_ref().cmp(want.as_str())))),
            _ => bad("a string"),
        },
        Lit::Bool(want) => match v {
            Leaf::Bool(b) => Ok(Tri::from_bool(op.holds(b.cmp(want)))),
            _ => bad("a boolean"),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Path(String),
    Str(String),
    Num(String),
    Cmp(Cmp),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    And,
    Or,
    Not,
    In,
    Is,
    Null,
    True,
    False,
}

struct Token {
    tok: Tok,
    pos: usize,
}

fn lex(s: &str) -> Result<Vec<Token>, ParseError> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let pos = i;
        let tok = match c {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
                continue;
            }
            b'(' => {
                i += 1;
                Tok::LParen
            }
            b')' => {
                i += 1;
                Tok::RParen
            }
            b'[' => {
                i += 1;
                Tok::LBracket
            }
            b']' => {
                i += 1;
                Tok::RBracket
            }
            b',' => {
                i += 1;
                Tok::Comma
            }
            b'=' if b.get(i + 1) == Some(&b'=') => {
                i += 2;
                Tok::Cmp(Cmp::Eq)
            }
            b'!' if b.get(i + 1) == Some(&b'=') => {
                i += 2;
                Tok::Cmp(Cmp::Ne)
            }
            b'<' if b.get(i + 1) == Some(&b'=') => {
                i += 2;
                Tok::Cmp(Cmp::Le)
            }
            b'>' if b.get(i + 1) == Some(&b'=') => {
                i += 2;
                Tok::Cmp(Cmp::Ge)
            }
            b'<' => {
                i += 1;
                Tok::Cmp(Cmp::Lt)
            }
            b'>' => {
                i += 1;
                Tok::Cmp(Cmp::Gt)
            }
            b'\'' => {
                // SQL-style string: '' is a quote.
                let mut text = String::new();
                i += 1;
                let mut start = i;
                loop {
                    match b.get(i) {
                        None => return perr(pos, "unterminated string"),
                        Some(b'\'') => {
                            text.push_str(s.get(start..i).unwrap_or_default());
                            if b.get(i + 1) == Some(&b'\'') {
                                text.push('\'');
                                i += 2;
                                start = i;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(_) => i += 1,
                    }
                }
                Tok::Str(text)
            }
            b'-' | b'+' | b'.' | b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric()
                        || b[i] == b'.'
                        || ((b[i] == b'+' || b[i] == b'-') && matches!(b[i - 1], b'e' | b'E')))
                {
                    i += 1;
                }
                Tok::Num(s.get(start..i).unwrap_or_default().to_string())
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.')
                {
                    i += 1;
                }
                let word = s.get(start..i).unwrap_or_default();
                match word.to_ascii_lowercase().as_str() {
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "in" => Tok::In,
                    "is" => Tok::Is,
                    "null" => Tok::Null,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    _ => Tok::Path(word.to_string()),
                }
            }
            _ => {
                return perr(
                    pos,
                    format!(
                        "unexpected character `{}`",
                        s.get(pos..).map_or('?', first_char)
                    ),
                )
            }
        };
        out.push(Token { tok, pos });
    }
    Ok(out)
}

fn first_char(s: &str) -> char {
    s.chars().next().unwrap_or('?')
}

// ---------------------------------------------------------------------------------------------
// Parser (recursive descent: or > and > not > primary)
// ---------------------------------------------------------------------------------------------

struct Parser<'t> {
    toks: &'t [Token],
    i: usize,
    depth: usize,
    end: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i).map(|t| &t.tok)
    }

    fn pos(&self) -> usize {
        self.toks.get(self.i).map_or(self.end, |t| t.pos)
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn enter(&mut self) -> Result<(), ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return perr(self.pos(), format!("nesting is deeper than {MAX_DEPTH}"));
        }
        Ok(())
    }

    fn or(&mut self) -> Result<Node, ParseError> {
        let mut v = vec![self.and()?];
        while self.eat(&Tok::Or) {
            v.push(self.and()?);
        }
        Ok(if v.len() == 1 {
            v.remove(0)
        } else {
            Node::Or(v)
        })
    }

    fn and(&mut self) -> Result<Node, ParseError> {
        let mut v = vec![self.not()?];
        while self.eat(&Tok::And) {
            v.push(self.not()?);
        }
        Ok(if v.len() == 1 {
            v.remove(0)
        } else {
            Node::And(v)
        })
    }

    fn not(&mut self) -> Result<Node, ParseError> {
        if self.eat(&Tok::Not) {
            self.enter()?;
            let inner = self.not();
            self.depth -= 1;
            return Ok(Node::Not(Box::new(inner?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Node, ParseError> {
        if self.eat(&Tok::LParen) {
            self.enter()?;
            let inner = self.or();
            self.depth -= 1;
            let inner = inner?;
            if !self.eat(&Tok::RParen) {
                return perr(self.pos(), "expected `)`");
            }
            return Ok(inner);
        }
        let pos = self.pos();
        let field = match self.peek() {
            Some(Tok::Path(p)) => {
                let f = FieldRef::parse(p).map_err(|m| ParseError { pos, msg: m })?;
                self.i += 1;
                f
            }
            Some(_) => {
                return perr(
                    pos,
                    "expected a field (op, schema, table, before.x, after.x, row.x) or `(`",
                )
            }
            None => return perr(pos, "unexpected end of expression"),
        };
        if self.eat(&Tok::Is) {
            let negated = self.eat(&Tok::Not);
            if !self.eat(&Tok::Null) {
                return perr(self.pos(), "expected `null` after `is`");
            }
            return Ok(Node::IsNull(field, negated));
        }
        if self.eat(&Tok::In) {
            if !self.eat(&Tok::LBracket) {
                return perr(self.pos(), "expected `[` after `in`");
            }
            let mut lits = vec![self.literal()?];
            while self.eat(&Tok::Comma) {
                lits.push(self.literal()?);
            }
            if !self.eat(&Tok::RBracket) {
                return perr(self.pos(), "expected `]`");
            }
            if let Some(first) = lits.first() {
                if let Some(other) = lits.iter().find(|l| l.kind() != first.kind()) {
                    return perr(
                        pos,
                        format!(
                            "`in` mixes a {} and a {}; all items must have one type",
                            first.kind(),
                            other.kind()
                        ),
                    );
                }
            }
            return Ok(Node::In(field, lits));
        }
        let op = match self.peek() {
            Some(Tok::Cmp(c)) => *c,
            _ => return perr(self.pos(), "expected a comparison, `in`, or `is null`"),
        };
        self.i += 1;
        let lit_pos = self.pos();
        if self.peek() == Some(&Tok::Null) {
            return perr(
                lit_pos,
                "comparing with `null` is always unknown: use `is null` or `is not null`",
            );
        }
        let lit = self.literal()?;
        if !matches!(op, Cmp::Eq | Cmp::Ne) {
            match &lit {
                Lit::Str(_) => {
                    return perr(
                        lit_pos,
                        "ordering a string is not supported (timestamp text does not sort reliably): use == / != / in",
                    )
                }
                Lit::Bool(_) => return perr(lit_pos, "a boolean only supports == and !="),
                Lit::Num(_) => {}
            }
        }
        Ok(Node::Cmp(field, op, lit))
    }

    fn literal(&mut self) -> Result<Lit, ParseError> {
        let pos = self.pos();
        let lit = match self.peek() {
            Some(Tok::Str(s)) => Lit::Str(s.clone()),
            Some(Tok::Num(n)) => match Dec::parse(n) {
                Some(d) => Lit::Num(d),
                None => return perr(pos, format!("`{n}` is not a valid number")),
            },
            Some(Tok::True) => Lit::Bool(true),
            Some(Tok::False) => Lit::Bool(false),
            Some(Tok::Null) => return perr(pos, "`null` is not a valid list item: use `is null`"),
            _ => return perr(pos, "expected a string, number, true or false"),
        };
        self.i += 1;
        Ok(lit)
    }
}

// ---------------------------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Lit(String),
    Field(FieldRef),
}

/// A string with `{field}` placeholders (`{schema}`, `{table}`, `{op}`, `{row.region}`, ...).
/// This only renders values. The topic rules (static prefix, one segment, allow-list, fallback)
/// belong to content routing and are checked there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    parts: Vec<Part>,
    source: String,
}

/// Why a template could not be rendered for one event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    /// The field is missing or `null`.
    Missing(String),
    /// The field holds an object or array.
    NotScalar(String),
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TemplateError::Missing(p) => write!(f, "`{p}` is missing or null"),
            TemplateError::NotScalar(p) => write!(f, "`{p}` is an object or array"),
        }
    }
}

impl std::error::Error for TemplateError {}

impl Template {
    pub fn parse(input: &str) -> Result<Template, ParseError> {
        if input.len() > MAX_EXPR_BYTES {
            return perr(0, format!("template is longer than {MAX_EXPR_BYTES} bytes"));
        }
        let mut parts = Vec::new();
        let mut lit = String::new();
        let mut chars = input.char_indices();
        while let Some((i, c)) = chars.next() {
            match c {
                '{' => {
                    let rest = input.get(i + 1..).unwrap_or_default();
                    let Some(len) = rest.find('}') else {
                        return perr(i, "unclosed `{`");
                    };
                    let name = rest.get(..len).unwrap_or_default();
                    if name.contains('{') {
                        return perr(i, "nested `{`");
                    }
                    let f = FieldRef::parse(name).map_err(|m| ParseError { pos: i, msg: m })?;
                    if !lit.is_empty() {
                        parts.push(Part::Lit(std::mem::take(&mut lit)));
                    }
                    parts.push(Part::Field(f));
                    // Skip past the closing brace.
                    for _ in 0..name.chars().count() + 1 {
                        chars.next();
                    }
                }
                '}' => return perr(i, "unmatched `}`"),
                c => lit.push(c),
            }
        }
        if !lit.is_empty() {
            parts.push(Part::Lit(lit));
        }
        Ok(Template {
            parts,
            source: input.to_string(),
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// True when the template has no placeholders.
    pub fn is_static(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Lit(_)))
    }

    /// Every field the template reads.
    pub fn fields(&self) -> impl Iterator<Item = &FieldRef> {
        self.parts.iter().filter_map(|p| match p {
            Part::Field(f) => Some(f),
            Part::Lit(_) => None,
        })
    }

    /// Fills the placeholders. Strings insert their content, numbers and booleans their JSON text.
    pub fn render(&self, view: &EventView<'_>) -> Result<String, TemplateError> {
        let mut out = String::new();
        for p in &self.parts {
            match p {
                Part::Lit(s) => out.push_str(s),
                Part::Field(f) => match view.value(f) {
                    Leaf::Null => return Err(TemplateError::Missing(f.text.clone())),
                    Leaf::Other => return Err(TemplateError::NotScalar(f.text.clone())),
                    Leaf::Bool(b) => out.push_str(if b { "true" } else { "false" }),
                    Leaf::Num(n) => out.push_str(n),
                    Leaf::Str(s) => out.push_str(&s),
                },
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Position;

    fn event(op: Op, before: Option<&str>, after: Option<&str>) -> ChangeEvent {
        ChangeEvent {
            position: Position::Lsn(1),
            xid: 1,
            commit_ts_micros: 0,
            schema: "public".into(),
            table: "orders".into(),
            op,
            before: before.and_then(|s| RawValue::from_string(s.into()).ok()),
            after: after.and_then(|s| RawValue::from_string(s.into()).ok()),
            key: None,
        }
    }

    fn ins(after: &str) -> ChangeEvent {
        event(Op::Insert, None, Some(after))
    }

    fn run(expr: &str, ev: &ChangeEvent) -> Result<Tri, EvalError> {
        Expr::parse(expr).unwrap().eval(&EventView::new(ev))
    }

    fn t(expr: &str, row: &str) -> Tri {
        run(expr, &ins(row)).unwrap()
    }

    fn is_type_error(expr: &str, row: &str) -> bool {
        run(expr, &ins(row)).is_err()
    }

    // --- parsing -------------------------------------------------------------------------

    #[test]
    fn parses_the_adr_example() {
        let e = Expr::parse("row.status == 'paid' or op == 'delete'").unwrap();
        let names: Vec<_> = e.fields().iter().map(|f| f.as_str()).collect();
        assert_eq!(names, ["row.status", "op"]);
    }

    #[test]
    fn syntax_errors_are_reported_with_a_position() {
        for bad in [
            "",
            "row.a ==",
            "row.a == 1 and",
            "(row.a == 1",
            "row.a == 1)",
            "row.a 1",
            "row.a in 1",
            "row.a in []",
            "row.a in [1,]",
            "row.a is 3",
            "row.a is not",
            "'x' == row.a",
            "row.a == 'unterminated",
            "row.a == 1 row.b == 2",
            "row.a @ 1",
        ] {
            assert!(Expr::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn field_paths_are_validated() {
        for bad in [
            "row", "before", "after.", "row..x", "foo.bar", "op.x", "table.a", "x",
        ] {
            assert!(
                Expr::parse(&format!("{bad} == 1")).is_err(),
                "should reject {bad:?}"
            );
        }
        for ok in [
            "op",
            "schema",
            "table",
            "before.a",
            "after.a.b.c",
            "row.status",
        ] {
            let probe = if matches!(ok, "op" | "schema" | "table") {
                format!("{ok} == 'x'")
            } else {
                format!("{ok} == 1")
            };
            assert!(Expr::parse(&probe).is_ok(), "should accept {ok:?}");
        }
    }

    #[test]
    fn comparing_with_null_is_a_startup_error() {
        assert!(Expr::parse("row.a == null").is_err());
        assert!(Expr::parse("row.a != null").is_err());
        assert!(Expr::parse("row.a in [null]").is_err());
    }

    #[test]
    fn ordering_strings_and_booleans_is_a_startup_error() {
        for bad in [
            "row.ts < '2024-01-01'",
            "row.name >= 'a'",
            "row.flag < true",
            "row.flag > false",
        ] {
            assert!(Expr::parse(bad).is_err(), "should reject {bad:?}");
        }
        assert!(Expr::parse("row.n < 3").is_ok());
    }

    #[test]
    fn in_lists_must_have_one_type() {
        assert!(Expr::parse("row.a in [1, 'x']").is_err());
        assert!(Expr::parse("row.a in ['x', true]").is_err());
        assert!(Expr::parse("row.a in [1, 2.5, -3]").is_ok());
    }

    #[test]
    fn keywords_are_case_insensitive_but_paths_are_not() {
        assert_eq!(
            t("row.a == 1 AND NOT row.b is NULL", r#"{"a":1,"b":2}"#),
            Tri::True
        );
        assert_eq!(t("row.A == 1", r#"{"a":1}"#), Tri::Unknown);
    }

    #[test]
    fn a_keyword_can_be_a_column_name_below_a_prefix() {
        assert_eq!(
            t("row.not == 1 and row.in == 2", r#"{"not":1,"in":2}"#),
            Tri::True
        );
    }

    #[test]
    fn strings_use_sql_quoting() {
        assert_eq!(t("row.n == 'it''s'", r#"{"n":"it's"}"#), Tri::True);
        assert_eq!(t("row.n == ''", r#"{"n":""}"#), Tri::True);
    }

    // --- limits --------------------------------------------------------------------------

    #[test]
    fn input_is_capped_at_4_kib() {
        let ok = format!("row.a == '{}'", "x".repeat(MAX_EXPR_BYTES - 20));
        assert!(Expr::parse(&ok).is_ok());
        let big = format!("row.a == '{}'", "x".repeat(MAX_EXPR_BYTES));
        assert!(Expr::parse(&big).is_err());
        assert!(Template::parse(&"a".repeat(MAX_EXPR_BYTES + 1)).is_err());
    }

    #[test]
    fn nesting_depth_is_capped_at_32() {
        let nest = |n: usize| format!("{}row.a == 1{}", "(".repeat(n), ")".repeat(n));
        assert!(Expr::parse(&nest(MAX_DEPTH)).is_ok());
        assert!(Expr::parse(&nest(MAX_DEPTH + 1)).is_err());
        let nots = |n: usize| format!("{}row.a == 1", "not ".repeat(n));
        assert!(Expr::parse(&nots(MAX_DEPTH)).is_ok());
        assert!(Expr::parse(&nots(MAX_DEPTH + 1)).is_err());
        // The cap holds well inside the byte limit, and a hostile depth never overflows the stack.
        assert!(Expr::parse(&nest(1000)).is_err());
        assert!(Expr::parse(&nots(1000)).is_err());
    }

    #[test]
    fn long_flat_chains_are_not_recursion() {
        let chain = vec!["row.a == 1"; 250].join(" and ");
        assert!(chain.len() <= MAX_EXPR_BYTES);
        let e = Expr::parse(&chain).unwrap();
        let ev = ins(r#"{"a":1}"#);
        assert_eq!(e.eval(&EventView::new(&ev)), Ok(Tri::True));
    }

    #[test]
    fn deterministic_fuzz_never_panics() {
        // xorshift over a token alphabet that includes deep nesting and malformed input.
        let alphabet: &[&str] = &[
            "(",
            ")",
            "not ",
            "and ",
            "or ",
            "row.a",
            "after.b.c",
            "before.d",
            "op",
            "==",
            "!=",
            "<",
            ">=",
            " ",
            "'x'",
            "'",
            "''",
            "1",
            "-2.5e3",
            "1e999999",
            ".",
            "in ",
            "[",
            "]",
            ",",
            "is ",
            "null ",
            "true ",
            "false ",
            "@",
            "\u{e9}",
            "\"",
        ];
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let ev = ins(r#"{"a":1,"b":{"c":"x"},"d":null}"#);
        let view = EventView::new(&ev);
        for _ in 0..4000 {
            let n = (next() % 60) as usize;
            let src: String = (0..n)
                .map(|_| alphabet[(next() as usize) % alphabet.len()])
                .collect();
            if let Ok(e) = Expr::parse(&src) {
                let _ = e.eval(&view);
            }
            let _ = Template::parse(&src);
        }
        // Pure nesting of every length up to past the byte limit.
        for n in (0..MAX_EXPR_BYTES).step_by(97) {
            let src = "(".repeat(n);
            assert!(Expr::parse(&src).is_err());
            let src = format!("{}row.a == 1{}", "(".repeat(n / 2), ")".repeat(n / 2));
            let ok = Expr::parse(&src).is_ok();
            assert_eq!(ok, n / 2 <= MAX_DEPTH && src.len() <= MAX_EXPR_BYTES);
        }
    }

    // --- three-valued logic --------------------------------------------------------------

    #[test]
    fn missing_field_is_null_and_comparison_with_null_is_unknown() {
        assert_eq!(t("row.region == 'eu'", r#"{"id":1}"#), Tri::Unknown);
        assert_eq!(t("row.region == 'eu'", r#"{"region":null}"#), Tri::Unknown);
        assert_eq!(t("row.n > 3", r#"{"n":null}"#), Tri::Unknown);
        assert_eq!(t("row.region in ['eu']", r#"{"id":1}"#), Tri::Unknown);
        assert_eq!(t("row.region is null", r#"{"id":1}"#), Tri::True);
        assert_eq!(t("row.region is not null", r#"{"id":1}"#), Tri::False);
        assert_eq!(t("row.region is null", r#"{"region":"eu"}"#), Tri::False);
    }

    #[test]
    fn truth_tables() {
        use Tri::*;
        // u: row.u is missing. t/f: row.a == 1 is true / row.a == 2 is false.
        let row = r#"{"a":1}"#;
        let cases = [
            ("not row.u == 1", Unknown),
            ("not row.a == 1", False),
            ("not row.a == 2", True),
            ("row.a == 2 and row.u == 1", False),
            ("row.a == 1 and row.u == 1", Unknown),
            ("row.a == 1 and row.a == 1", True),
            ("row.a == 1 or row.u == 1", True),
            ("row.a == 2 or row.u == 1", Unknown),
            ("row.a == 2 or row.a == 2", False),
            ("row.u == 1 and row.u == 1", Unknown),
            ("row.u == 1 or row.u == 1", Unknown),
        ];
        for (expr, want) in cases {
            assert_eq!(t(expr, row), want, "{expr}");
        }
    }

    #[test]
    fn unknown_is_filtered_by_matches() {
        let ev = ins(r#"{"id":1}"#);
        let e = Expr::parse("not (row.region == 'eu')").unwrap();
        assert_eq!(e.eval(&EventView::new(&ev)), Ok(Tri::Unknown));
        assert_eq!(e.matches(&EventView::new(&ev)), Ok(false));
        let keep = Expr::parse("row.region is null or not (row.region == 'eu')").unwrap();
        assert_eq!(keep.matches(&EventView::new(&ev)), Ok(true));
    }

    // --- typed comparison ----------------------------------------------------------------

    #[test]
    fn number_literal_against_json_numbers() {
        assert_eq!(t("row.n == 42", r#"{"n":42}"#), Tri::True);
        assert_eq!(t("row.n > 41", r#"{"n":42}"#), Tri::True);
        assert_eq!(t("row.n <= 41.999", r#"{"n":42}"#), Tri::False);
        assert_eq!(t("row.n == 1.5", r#"{"n":1.5e0}"#), Tri::True);
        assert_eq!(t("row.n == 1000", r#"{"n":1e3}"#), Tri::True);
        assert_eq!(t("row.n < 0", r#"{"n":-0.001}"#), Tri::True);
    }

    #[test]
    fn number_literal_against_numeric_text_is_exact_decimal() {
        // numeric columns arrive as JSON strings
        assert_eq!(t("row.price == 10.5", r#"{"price":"10.50"}"#), Tri::True);
        assert_eq!(t("row.price > 10.49", r#"{"price":"10.50"}"#), Tri::True);
        assert_eq!(t("row.price < 10.5", r#"{"price":"10.50"}"#), Tri::False);
        assert_eq!(t("row.price == 0", r#"{"price":"-0.00"}"#), Tri::True);
        assert_eq!(t("row.price == 100", r#"{"price":"1E+2"}"#), Tri::True);
        // beyond f64 precision: 2^53 and 2^53 + 1 differ exactly
        assert_eq!(
            t("row.n == 9007199254740993", r#"{"n":"9007199254740992"}"#),
            Tri::False
        );
        assert_eq!(
            t("row.n > 9007199254740992", r#"{"n":"9007199254740993"}"#),
            Tri::True
        );
        assert_eq!(
            t(
                "row.n == 0.1234567890123456789",
                r#"{"n":"0.1234567890123456789"}"#
            ),
            Tri::True
        );
    }

    #[test]
    fn decimal_ordering_across_signs_and_magnitudes() {
        let cases = [
            ("-5", "3", Ordering::Less),
            ("3", "-5", Ordering::Greater),
            ("-5", "-3", Ordering::Less),
            ("-3", "-5", Ordering::Greater),
            ("0", "-0", Ordering::Equal),
            ("0", "0.0001", Ordering::Less),
            ("0", "-0.0001", Ordering::Greater),
            ("9", "10", Ordering::Less),
            ("0.9", "0.10", Ordering::Greater),
            ("12", "1.2e1", Ordering::Equal),
            ("100", "1e2", Ordering::Equal),
            ("1e-3", "0.0011", Ordering::Less),
            ("123", "1234", Ordering::Less),
        ];
        for (a, b, want) in cases {
            let (da, db) = (Dec::parse(a).unwrap(), Dec::parse(b).unwrap());
            assert_eq!(da.cmp(&db), want, "{a} vs {b}");
            assert_eq!(db.cmp(&da), want.reverse(), "{b} vs {a}");
        }
    }

    #[test]
    fn decimal_parse_rejects_non_numbers() {
        for bad in [
            "",
            " ",
            "NaN",
            "nan",
            "Infinity",
            "-Infinity",
            "inf",
            "1e",
            "e5",
            "--1",
            "1.2.3",
            "0x10",
            "1 ",
            " 1",
            "1_000",
            "+",
            "-",
            ".",
        ] {
            assert!(Dec::parse(bad).is_none(), "should reject {bad:?}");
        }
        assert!(Dec::parse(&"9".repeat(MAX_DIGITS + 1)).is_none());
        assert!(Dec::parse("1e100001").is_none());
        assert!(Dec::parse("5.").is_some());
        assert!(Dec::parse(".5").is_some());
    }

    #[test]
    fn nan_and_infinity_are_type_errors_not_unknown() {
        for v in ["NaN", "Infinity", "-Infinity"] {
            let row = format!(r#"{{"n":"{v}"}}"#);
            assert!(is_type_error("row.n > 1", &row), "{v}");
            assert!(is_type_error("row.n == 1", &row), "{v}");
            assert!(is_type_error("row.n in [1, 2]", &row), "{v}");
        }
    }

    #[test]
    fn number_literal_against_other_types_is_a_type_error() {
        for row in [
            r#"{"n":"abc"}"#,
            r#"{"n":true}"#,
            r#"{"n":{"x":1}}"#,
            r#"{"n":[1]}"#,
            r#"{"n":""}"#,
        ] {
            assert!(is_type_error("row.n == 1", row), "{row}");
        }
    }

    #[test]
    fn string_literal_only_matches_strings() {
        assert_eq!(t("row.s == 'a'", r#"{"s":"a"}"#), Tri::True);
        assert_eq!(t("row.s != 'a'", r#"{"s":"b"}"#), Tri::True);
        assert_eq!(t("row.s in ['a','b']", r#"{"s":"b"}"#), Tri::True);
        assert_eq!(t("row.s in ['a','b']", r#"{"s":"c"}"#), Tri::False);
        for row in [
            r#"{"s":1}"#,
            r#"{"s":true}"#,
            r#"{"s":{"a":1}}"#,
            r#"{"s":[]}"#,
        ] {
            assert!(is_type_error("row.s == 'a'", row), "{row}");
        }
        // JSON escapes are decoded before comparing
        assert_eq!(t("row.s == 'a\"b'", r#"{"s":"a\"b"}"#), Tri::True);
        assert_eq!(t("row.s == 'é'", r#"{"s":"é"}"#), Tri::True);
    }

    #[test]
    fn bool_literal_only_matches_json_bools() {
        assert_eq!(t("row.f == true", r#"{"f":true}"#), Tri::True);
        assert_eq!(t("row.f != true", r#"{"f":false}"#), Tri::True);
        assert_eq!(t("row.f == false", r#"{"f":false}"#), Tri::True);
        for row in [r#"{"f":"true"}"#, r#"{"f":1}"#, r#"{"f":{}}"#] {
            assert!(is_type_error("row.f == true", row), "{row}");
        }
    }

    // --- errors win ----------------------------------------------------------------------

    #[test]
    fn a_type_error_wins_over_every_truth_value_with_no_short_circuit() {
        let row = r#"{"a":1,"s":"abc"}"#;
        for expr in [
            "row.a == 2 and row.s > 1", // false and error
            "row.a == 1 or row.s > 1",  // true or error
            "row.a == 1 and row.s > 1", // true and error
            "row.a == 2 or row.s > 1",  // false or error
            "row.u == 1 and row.s > 1", // unknown and error
            "row.u == 1 or row.s > 1",  // unknown or error
            "not row.s > 1",            // not error
            "row.s > 1 and row.a == 2", // error on the left
            "not (row.a == 1 or row.s > 1)",
        ] {
            let err = run(expr, &ins(row)).unwrap_err();
            assert_eq!(err.path, "row.s", "{expr}");
        }
    }

    #[test]
    fn the_first_error_is_reported() {
        let ev = ins(r#"{"x":"a","y":"b"}"#);
        let err = Expr::parse("row.x > 1 and row.y > 1")
            .unwrap()
            .eval(&EventView::new(&ev))
            .unwrap_err();
        assert_eq!(err.path, "row.x");
        assert_eq!(err.expected, "a finite number");
    }

    #[test]
    fn error_text_never_contains_the_value() {
        let ev = ins(r#"{"card":"4111-1111-1111-1111"}"#);
        let err = Expr::parse("row.card > 1")
            .unwrap()
            .eval(&EventView::new(&ev))
            .unwrap_err();
        let text = err.to_string();
        assert!(!text.contains("4111"));
        assert_eq!(text, "type error: `row.card` is not a finite number");
    }

    // --- fields --------------------------------------------------------------------------

    #[test]
    fn op_schema_and_table() {
        let ev = event(Op::Delete, Some(r#"{"id":1}"#), None);
        assert_eq!(run("op == 'delete'", &ev), Ok(Tri::True));
        assert_eq!(run("op in ['insert','update']", &ev), Ok(Tri::False));
        assert_eq!(
            run("schema == 'public' and table == 'orders'", &ev),
            Ok(Tri::True)
        );
        assert_eq!(
            run("op == 'truncate'", &event(Op::Truncate, None, None)),
            Ok(Tri::True)
        );
    }

    #[test]
    fn row_is_after_else_before() {
        let upd = event(Op::Update, Some(r#"{"s":"old"}"#), Some(r#"{"s":"new"}"#));
        assert_eq!(run("row.s == 'new'", &upd), Ok(Tri::True));
        assert_eq!(run("before.s == 'old'", &upd), Ok(Tri::True));
        assert_eq!(run("after.s == 'new'", &upd), Ok(Tri::True));
        let del = event(Op::Delete, Some(r#"{"s":"old"}"#), None);
        assert_eq!(run("row.s == 'old'", &del), Ok(Tri::True));
        assert_eq!(run("after.s == 'old'", &del), Ok(Tri::Unknown));
        let trunc = event(Op::Truncate, None, None);
        assert_eq!(run("row.s == 'old'", &trunc), Ok(Tri::Unknown));
    }

    #[test]
    fn dotted_paths_read_nested_documents() {
        let row = r#"{"address":{"city":"Ghent","geo":{"lat":51.05}},"tags":["a"]}"#;
        assert_eq!(t("after.address.city == 'Ghent'", row), Tri::True);
        assert_eq!(t("after.address.geo.lat > 51", row), Tri::True);
        assert_eq!(t("after.address.zip is null", row), Tri::True);
        assert_eq!(t("after.address.city.x is null", row), Tri::True);
        assert_eq!(t("after.nothing.city is null", row), Tri::True);
        // an object where a scalar is expected is a type error
        assert!(is_type_error("after.address == 'x'", row));
        assert!(is_type_error("after.tags == 'a'", row));
    }

    #[test]
    fn keys_with_escapes_are_found() {
        assert_eq!(t("row.a == 1", r#"{"a":1}"#), Tri::True);
    }

    #[test]
    fn present_distinguishes_missing_from_null() {
        let ev = ins(r#"{"a":null,"b":{"c":1}}"#);
        let view = EventView::new(&ev);
        let f = |s| FieldRef::parse(s).unwrap();
        assert!(view.present(&f("row.a")));
        assert!(view.present(&f("row.b.c")));
        assert!(!view.present(&f("row.z")));
        assert!(!view.present(&f("row.b.z")));
        assert!(view.present(&f("op")));
        assert!(!view.present(&f("before.a")));
    }

    #[test]
    fn rows_that_are_not_objects_read_as_null() {
        let ev = ins(r#"[1,2]"#);
        assert_eq!(
            Expr::parse("row.a is null")
                .unwrap()
                .eval(&EventView::new(&ev)),
            Ok(Tri::True)
        );
    }

    #[test]
    fn fields_lists_every_path() {
        let e = Expr::parse("(row.a == 1 or not before.b.c is null) and op in ['x']").unwrap();
        let names: Vec<_> = e.fields().iter().map(|f| f.as_str()).collect();
        assert_eq!(names, ["row.a", "before.b.c", "op"]);
        assert_eq!(e.fields()[0].column(), Some("a"));
        assert_eq!(e.fields()[2].column(), None);
    }

    // --- templates -----------------------------------------------------------------------

    fn render(tpl: &str, ev: &ChangeEvent) -> Result<String, TemplateError> {
        Template::parse(tpl).unwrap().render(&EventView::new(ev))
    }

    #[test]
    fn templates_fill_placeholders() {
        let ev = ins(r#"{"region":"eu","n":7,"f":true,"d":{"x":1}}"#);
        assert_eq!(
            render("cdc.{schema}.{table}", &ev).as_deref(),
            Ok("cdc.public.orders")
        );
        assert_eq!(render("{op}", &ev).as_deref(), Ok("insert"));
        assert_eq!(
            render("orders.{row.region}", &ev).as_deref(),
            Ok("orders.eu")
        );
        assert_eq!(
            render("n{after.n}-{after.f}", &ev).as_deref(),
            Ok("n7-true")
        );
        assert_eq!(render("static", &ev).as_deref(), Ok("static"));
        assert_eq!(render("", &ev).as_deref(), Ok(""));
    }

    #[test]
    fn template_misses_are_typed() {
        let ev = ins(r#"{"region":null,"d":{"x":1}}"#);
        assert_eq!(
            render("a.{row.region}", &ev),
            Err(TemplateError::Missing("row.region".into()))
        );
        assert_eq!(
            render("a.{row.nope}", &ev),
            Err(TemplateError::Missing("row.nope".into()))
        );
        assert_eq!(
            render("a.{row.d}", &ev),
            Err(TemplateError::NotScalar("row.d".into()))
        );
    }

    #[test]
    fn template_syntax_errors() {
        for bad in [
            "a.{",
            "a.{row.x",
            "a.}",
            "a.{{row.x}}",
            "{}",
            "{nope}",
            "{row}",
            "{row.}",
        ] {
            assert!(Template::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn template_introspection() {
        let t = Template::parse("o.{row.a}.{table}").unwrap();
        assert!(!t.is_static());
        let names: Vec<_> = t.fields().map(|f| f.as_str()).collect();
        assert_eq!(names, ["row.a", "table"]);
        assert!(Template::parse("plain.topic").unwrap().is_static());
    }

    #[test]
    fn template_handles_multibyte_literals() {
        let ev = ins(r#"{"a":"x"}"#);
        assert_eq!(render("é.{row.a}.ü", &ev).as_deref(), Ok("é.x.ü"));
        assert_eq!(render("{row.a}é{row.a}", &ev).as_deref(), Ok("xéx"));
    }
}
