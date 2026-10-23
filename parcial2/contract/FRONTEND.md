# Frontend contract (selectors)

The harness drives a real browser (Playwright). Use these `data-testid` attributes; layout and style are free.

Customer flow (`frontend.base_url` + `/`):
`search-origin` (input), `search-destination` (input), `search-date` (input type=date or text YYYY-MM-DD),
`search-submit` (button), `flight-row` (one per result, each containing `reserve-button`),
`passenger-name` (input, appears after clicking reserve), `confirm-submit` (button),
`confirmation-id` (element whose text is exactly the reservation_id).

Manager panel (`/manager`):
`manager-route` (input), `manager-month` (input, YYYY-MM), `manager-load` (button),
`occupancy-value` (text like `78.4%`, at least one decimal).
