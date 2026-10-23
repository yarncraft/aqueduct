- **The cockpit no longer needs inline scripts** (#218). The dashboard script is now served from `/admin.js`, and the
  `Content-Security-Policy` is `script-src 'self'` instead of `'unsafe-inline'`, so an injected inline script cannot run.
  Inline styles stay allowed (bar widths and colours are set from data). Nothing to do on upgrade.
