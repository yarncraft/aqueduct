//! Keeps credentials out of `Debug` output and so out of logs.

/// `url` with its password replaced by `<redacted>`: the `user:password@` part of a URL
/// (`postgres://`, `mongodb://`, `mongodb+srv://`, ...) and a `password=` key of a libpq
/// `key=value` connection string. Everything else is kept, so the result still says where the
/// connection goes.
pub fn redact_password(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        // The authority ends at the first '/', '?' or '#'; the last '@' in it ends the userinfo.
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        let mut out = url[..scheme_end + 3].to_string();
        match authority.rfind('@') {
            Some(at) => {
                let userinfo = &authority[..at];
                match userinfo.split_once(':') {
                    Some((user, _)) => out.push_str(&format!("{user}:<redacted>")),
                    None => out.push_str(userinfo),
                }
                out.push_str(&authority[at..]);
            }
            None => out.push_str(authority),
        }
        out.push_str(&redact_query(&rest[authority_end..]));
        return out;
    }
    url.split(' ')
        .map(|kv| match kv.split_once('=') {
            Some((k, _)) if k.eq_ignore_ascii_case("password") => format!("{k}=<redacted>"),
            _ => kv.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A `password=` query parameter, as libpq URLs allow.
fn redact_query(tail: &str) -> String {
    let Some(q) = tail.find('?') else {
        return tail.to_string();
    };
    let (path, query) = tail.split_at(q + 1);
    let query = query
        .split('&')
        .map(|kv| match kv.split_once('=') {
            Some((k, _)) if k.eq_ignore_ascii_case("password") => format!("{k}=<redacted>"),
            _ => kv.to_string(),
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}{query}")
}

#[cfg(test)]
mod tests {
    use super::redact_password as r;

    #[test]
    fn passwords_are_hidden_and_the_rest_is_kept() {
        assert_eq!(
            r("postgres://app:s3cr%40t@db:5432/app?sslmode=verify-full"),
            "postgres://app:<redacted>@db:5432/app?sslmode=verify-full"
        );
        assert_eq!(r("postgres://app@db/app"), "postgres://app@db/app");
        assert_eq!(r("postgres://db/app"), "postgres://db/app");
        assert_eq!(
            r("postgres://app@db/app?password=hunter2&sslmode=require"),
            "postgres://app@db/app?password=<redacted>&sslmode=require"
        );
        assert_eq!(
            r("mongodb+srv://svc:p%3Aw@cluster0.example.net/?retryWrites=true"),
            "mongodb+srv://svc:<redacted>@cluster0.example.net/?retryWrites=true"
        );
        assert_eq!(
            r("host=db user=app password=hunter2 dbname=app"),
            "host=db user=app password=<redacted> dbname=app"
        );
        // An '@' in the path is not userinfo.
        assert_eq!(r("postgres://db/a@b"), "postgres://db/a@b");
    }
}
