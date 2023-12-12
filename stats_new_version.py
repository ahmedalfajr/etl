import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('sales_data.csv', sep=";")
df['QuantityOrdered'] = pd.to_numeric(df['QuantityOrdered'], errors='coerce')
df['PriceEach'] = pd.to_numeric(df['PriceEach'], errors='coerce')
print("First 5 rows of the dataset:")
print(df.head())
missing_values = df.isnull().sum()
print("\nMissing values in the dataset:")
print(missing_values)
df['TotalSales'] = df['QuantityOrdered'].astype(float) * df['PriceEach'].astype(float)
print(df.head())
total_sales = df['TotalSales'].sum()
print("\nTotal sales across all orders:", total_sales)
highest_sales_product = df.groupby('ProductCode')['TotalSales'].sum().idxmax()
print("\nProduct with the highest total sales:", highest_sales_product)
average_sales_per_order = df.groupby('OrderID')['TotalSales'].sum().mean()
print("\nAverage sales per order:", average_sales_per_order)
city_with_highest_orders = df['City'].value_counts().idxmax()
highest_city_orders = df[df['City'] == city_with_highest_orders]
unique_orders_in_highest_city = highest_city_orders['OrderID'].unique()
print(f"Unique orders in the highest city ({city_with_highest_orders}):")
print(unique_orders_in_highest_city.size)
print("\nCity with the highest number of orders:", city_with_highest_orders)
df.to_csv('modified_sales_data.csv', index=False)

total_sales_per_city = df.groupby('City')['TotalSales'].sum()
total_sales_per_city.plot(kind='bar', figsize=(10, 6))
plt.title('Total Sales per City')
plt.xlabel('City')
plt.ylabel('Total Sales')
plt.show()



