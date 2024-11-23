
from datetime import datetime
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from src.backend.backend import sectors_list, driver, get_data_for_risk_repartition, get_data_nb_controverties_distrib, get_data_financial_impact_by_controversy_per_sector, get_articles_for_sector_controversy, get_nb_controversies_per_activity
st.set_page_config(layout="wide")
session = driver.session()

# Streamlit App
st.title("ControVert.ia")

tab1, tab2 = st.tabs(["Overview", "Focus sur un secteur"])

with tab1:
    st.subheader("Overview")

    overview_data = get_nb_controversies_per_activity(session)

    activity_nb_articles_fig = px.bar(
        overview_data.sort_values("number_of_articles", ascending=True),
        x="number_of_articles",
        y="activity",
        orientation="h",
        color="number_of_articles",
        color_continuous_scale="Reds",
        labels={"number_of_articles": "Nombre d'articles", "activity": "Secteur d'activité"},
    )
    activity_nb_articles_fig.update_layout(title="Nombre d'articles par secteur d'activité")
    st.plotly_chart(activity_nb_articles_fig, use_container_width=True)


    activity_financial_fig = px.bar(
        overview_data.dropna(subset="min_perf_diff_2_months").sort_values("min_perf_diff_2_months", ascending=False),
        x="min_perf_diff_2_months",
        y="activity",
        orientation="h",
        color="min_perf_diff_2_months",
        color_discrete_sequence=px.colors.sequential.Reds.reverse(),
        labels={"min_perf_diff_2_months": "Impact boursier sur 2 mois(%)", "activity": "Secteur d'activité"},
    )
    activity_financial_fig.update_layout(title="Impact boursier minimal sur 2 mois par secteur d'activité")
    st.plotly_chart(activity_financial_fig, use_container_width=True)


with tab2:

    # Search Bar for Sector
    sector_filter = st.selectbox("Selectionner un secteur:", sectors_list, index=0)


    data_pie_chart = get_data_for_risk_repartition(session,sector_filter)
    data_bar_chart = get_data_nb_controverties_distrib(session)
    # Clip number of articles to 2* nb_articles of the selected sector
    value = data_bar_chart.loc[data_bar_chart["sector_name"] == sector_filter, "number_of_articles"].values[0]
    data_bar_chart["number_of_articles"] = data_bar_chart["number_of_articles"].clip(0, 2 * value)
    data_financial_impact = get_data_financial_impact_by_controversy_per_sector(session,sector_filter)



    # Split into two columns
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Proportion du nombre d'articles par risque")
        if data_pie_chart is not None and not data_pie_chart.empty:
            # Count the number of articles per risk for the selected sector
            data_pie_chart.rename(columns={"controversy_name": "Risk", "number_of_articles": "Article Count"}, inplace=True)

            # Create a pie chart using Plotly with a qualitative palette
            pie_fig = px.pie(
                data_pie_chart,
                names="Risk",
                values="Article Count",
                color="Risk",
            )
            pie_fig.update_layout(title=" ")  # Set title to empty string

            # Display the pie chart in Streamlit
            st.plotly_chart(pie_fig, use_container_width=True)
        else:
            st.write("No articles available for the selected sector.")

    with col2:
        st.subheader("Nombre d'articles par secteur")
        sector_counts = data_bar_chart
        sector_counts.columns = ["Sector", "Count"]

        # Group by the exact number of articles and count the number of sectors per count
        exact_data = sector_counts.groupby("Count").size().reset_index(name="Sector Count")

        # Highlight the selected sector's count
        selected_sector_count = sector_counts.loc[
            sector_counts["Sector"] == sector_filter, "Count"
        ].values[0]

        # Use Plotly's Redor color palette for the bar chart
        color_palette = px.colors.sequential.Redor
        max_count = exact_data["Count"].max()
        min_count = exact_data["Count"].min()

        def get_color(value):
            normalized = (
                (value - min_count) / (max_count - min_count)
                if max_count > min_count
                else 0
            )
            index = int(normalized * (len(color_palette) - 1))
            return color_palette[index]

        # Assign colors to bars
        exact_data["Color"] = exact_data["Count"].apply(get_color)

        # Create the bar chart
        bar_fig = go.Figure()

        for _, row in exact_data.iterrows():
            bar_fig.add_trace(
                go.Bar(
                    x=[row["Count"]],
                    y=[row["Sector Count"]],
                    marker=dict(
                        color=row["Color"],
                        line=dict(
                            color=("rgba(0,0,0,0)"),
                            width=(0
                            ),  # Thicker outline
                        ),
                    ),
                    text=f"{row['Sector Count']} sectors",
                    hoverinfo="x+y+text",
                )
            )
        # Add a red bar at the position of selected_sector_count
        bar_fig.add_trace(
            go.Bar(
                x=[selected_sector_count],  # Place the red bar at the selected count
                y=[max(exact_data["Sector Count"])],  # Set the height of the bar (adjust as necessary)
                name="Selected Sector Count",  # Optional: Add a name to the red bar
                marker=dict(
                    color="red",  # Red color for the selected bar
                    line=dict(
                        color="darkred",  # Dark red outline for the selected bar
                        width=6,  # Thicker border for the red bar
                    ),
                ),
                text="Selected Sector Count",  # Text for the selected sector
                hoverinfo="x+y+text",
                opacity=0.6,  # Optional: Reduce opacity to make it less overpowering
            )
        )

        # Update layout for the bar chart and set the title to empty string
        bar_fig.update_layout(
            title=" ",  # Set title to empty string
            xaxis_title="Nombre d'articles",
            yaxis_title="Nombre de secteurs",
            showlegend=False,
            bargap=0.1,
        )

        # Display the bar chart in Streamlit
        st.plotly_chart(bar_fig, use_container_width=True)

    # Horizontal Bar Chart: Maximum Financial Loss by Risk
    st.subheader(f"Impact financier maximal par risque pour le secteur: {sector_filter}")
    if data_financial_impact is not None and not data_financial_impact.empty:
        # Calculate the maximum financial loss for each risk
        data_financial_impact.rename({"controversy": "Risk", "perf": "Financial Impact (%)"}, axis=1, inplace=True)

        # Sort the data in descending order of financial loss
        max_losses = data_financial_impact.sort_values(by="Financial Impact (%)", ascending=True)

        # Create a horizontal bar chart
        loss_fig = px.bar(
            max_losses,
            x="Financial Impact (%)",
            y="Risk",
            orientation="h",
            color="Risk",
            labels={"Financial Impact (%)": "Impact financier maximal (%)", "Risk": "Type de risque"},
            color_discrete_sequence=px.colors.diverging.RdYlBu,
        )

        # Update layout and set the title to empty string
        loss_fig.update_layout(
            title=" ",  # Set title to empty string
            xaxis_title="Impact financier",
            yaxis_title="Niveau de risque",
            showlegend=False,
        )

        # Display the horizontal bar chart
        st.plotly_chart(loss_fig, use_container_width=True)
    else:
        st.write("No financial impact data available for the selected sector.")

    # Show a table of the first 10 articles in the selected sector
    st.subheader("Articles liés aux plus chutes boursières")
    articles_data = get_articles_for_sector_controversy(session,sector_filter)

    if articles_data is not None and not articles_data.empty:
        filtered_df = articles_data.assign(url=lambda df: df["article"].apply(lambda x: x["url"]), name=lambda df: df["article"].apply(lambda x: x["name"]))
        filtered_df = filtered_df.assign(markdown_name= lambda df: df.apply(lambda x: f'[{x["name"]}]({x["url"]})', axis=1))
        filtered_df = filtered_df.assign(date= lambda df: df.apply(lambda x: datetime.strptime(str(x["date"]), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d"), axis=1))
        top_articles = filtered_df[["markdown_name", "controversy", "perf_1", "perf_2", "date"]].sort_values("perf_1", ascending=True).head(10)
        top_articles = top_articles.rename(columns={"perf_1": "Impact sur le prix de l'action à 1 mois (%)", "perf_2": "Impact sur le prix de l'action à 2 mois (%)", "controversy": "Risque", "markdown_name": "Titre"})
        # st.table(top_articles)  # Display the table of articles
        df_md = top_articles.to_markdown(index=False)

        st.markdown(df_md)
    else:
        st.write("No articles available for the selected sector.")

    # Add Google Trends Insights Section
    st.markdown("---")
    st.subheader("Google Trends Insights")

    # Collect keywords and settings from user
    trend_keywords = st.text_input(
        "Enter keywords (comma-separated):", value="Scandale orpea,scandale renault"
    )
    region = st.selectbox("Select Region:", options=["Worldwide", "US", "FR"], index=0)
    time_range = st.selectbox(
        "Select Time Range:", options=["today 1-m", "today 12-m", "today 5-y"], index=2
    )

    # Format keywords for the embedding code
    keywords_list = trend_keywords.split(",")
    comparison_items = ",".join(
        [
            f'{{"keyword":"{kw.strip()}","geo":"{"" if region == "Worldwide" else region}","time":"{time_range}"}}'
            for kw in keywords_list
        ]
    )
    explore_query = f'date={time_range}&geo={"" if region == "Worldwide" else region}&q={",".join([kw.strip().replace(" ", "%20") for kw in keywords_list])}'

    # Embed Google Trends widget
    trends_html = f"""
    <script type="text/javascript" src="https://ssl.gstatic.com/trends_nrtr/3898_RC01/embed_loader.js"></script>
    <script type="text/javascript">
    trends.embed.renderExploreWidget("TIMESERIES", 
    {{"comparisonItem":[{comparison_items}],"category":0,"property":""}}, 
    {{"exploreQuery":"{explore_query}","guestPath":"https://trends.google.com:443/trends/embed/"}});
    </script>
    """

    st.components.v1.html(trends_html, height=500)