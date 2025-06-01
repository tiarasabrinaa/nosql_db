import matplotlib.pyplot as plt

def generate_chart(df, chart_type, column):
    fig, ax = plt.subplots()
    if chart_type == "Bar Chart":
        df[column].value_counts().plot(kind='bar', ax=ax)
        ax.set_ylabel('Count')
        ax.set_title(f'Bar Chart of {column}')
    elif chart_type == "Pie Chart":
        df[column].value_counts().plot(kind='pie', ax=ax, autopct='%1.1f%%')
        ax.set_ylabel('')
        ax.set_title(f'Pie Chart of {column}')
    elif chart_type == "Line Chart":
        df[column].reset_index(drop=True).plot(kind='line', ax=ax)
        ax.set_ylabel(column)
        ax.set_title(f'Line Chart of {column}')
    elif chart_type == "Histogram":
        df[column].plot(kind='hist', ax=ax, bins=20)
        ax.set_xlabel(column)
        ax.set_title(f'Histogram of {column}')
    else:
        st.warning("Unsupported chart type selected.")
        return None
    return fig
