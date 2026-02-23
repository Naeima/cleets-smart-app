import dash
from dash import html, dcc, page_container

# Create Dash app FIRST
app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    title="CLEETS-SMART Dashboard"
)

# Then import pages (each will call register_page now that app exists)
import pages.home
import pages.weather
import pages.westmidlands
import pages.heat_uk
import pages.ev_travel_planning
import pages.thrust_one
# Layout
app.layout = html.Div([
    dcc.Location(id="url"),
    page_container
])

if __name__ == "__main__":
    app.run(debug=True, port=8050)
