- **Cockpit: each table's event rate in the Routing card, on taller table and topic cards** (#166). Table cards (the
  source side) now carry a 120 s step line of their events per second, like routes and topics, and a "+N more" table
  group the sum. Table and topic cards are taller, with the step line on its own line under the name and count.
  Clicking a table opens a panel with a larger chart, the current, average and maximum rate, the route that claims it
  and its topic. Its chart draws one events-per-second line, with a toggle button per operation to choose what it counts. The per-table history is
  the one the stream already carries (`history_ops`); no API change.
