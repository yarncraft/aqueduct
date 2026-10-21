- **Cockpit: route panels show only what the card does not, with source buttons.** A route's detail panel lists the
  matched count, produced per operation, `topic_fallback`, `topic_values`, the topics a template wrote, the dead-letter
  topic and the full table list; its chart shows the route's processed, matched, fallback and errors. `ON ERROR` is no longer on the card, and dead-letter topics without events are not drawn. Fixed: routes
  that pick their topic per row now animate their lines to the topics they write.
