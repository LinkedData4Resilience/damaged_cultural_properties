import pandas as pd
import matplotlib.pyplot as plt

# Load the data
data = pd.read_csv('Query.csv')  # Replace 'your_file.csv' with the path to your file

# Rename the columns for easier handling
data.columns = ['EventID', 'EventType', 'EventDate']

# Convert the EventDate column to datetime, handling possible date format issues
data['EventDate'] = pd.to_datetime(data['EventDate'], errors='coerce')

# Filter out rows where EventDate could not be parsed (NaT values)
filtered_data = data.dropna(subset=['EventDate'])

# Extract year and month from the date
filtered_data['YearMonth'] = filtered_data['EventDate'].dt.to_period('M')

# Filter the data to keep only the specified event types: library, museum, and monuments
selected_types = [
    'https://linked4resilience.eu/data/library', 
    'https://linked4resilience.eu/data/museum', 
    'https://linked4resilience.eu/data/education', 
    'https://linked4resilience.eu/data/religious-site'
]
filtered_selected_data = filtered_data[filtered_data['EventType'].isin(selected_types)]

# Group the filtered data by EventType and YearMonth, and count the number of events
selected_grouped_data = filtered_selected_data.groupby(['YearMonth', 'EventType']).size().unstack(fill_value=0)

# Plot the filtered data with only the selected types
plt.figure(figsize=(6, 3))
selected_grouped_data.plot(kind='line', marker='o', figsize=(6, 3))

# Define a mapping from the old labels (URLs) to new labels
label_mapping = {
    'https://linked4resilience.eu/data/library': 'Library',
    'https://linked4resilience.eu/data/education': 'E&R',
    'https://linked4resilience.eu/data/museum': 'Museum',
    'https://linked4resilience.eu/data/religious-site': 'Religious sites'
}

# Update the legend with new labels
new_labels = [label_mapping[label] for label in selected_grouped_data.columns]
plt.legend(new_labels, title='Event Type', bbox_to_anchor=(1.05, 1), loc='upper left')

# plt.title('Event Trend by Type Over Time (Library, Museum, Monuments)')
plt.xlabel('Month')
plt.ylabel('Number of Events')
plt.grid(True)
plt.tight_layout()

# Show the plot
# Save the plot locally as a PNG file
plt.savefig('event_trend_plot.png', dpi=300)  # You can specify the path and file name

# Show the plot
plt.show()
