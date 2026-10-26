- **Postgres: the SQLSTATE of an SQL-connection error moved to the end** (#258). An error the server returned on the
  SQL connection now reads `…: <message> (<SQLSTATE>)`, like the replication connection's, instead of
  `…: <SQLSTATE> <message>`. The code is now kept as a field too, and the retry while a previous connection still
  holds the slot checks it (`55006`) instead of the message text. Update any log alert that matches the old order.
