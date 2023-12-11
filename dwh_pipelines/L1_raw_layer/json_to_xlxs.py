import pandas as pd

json_data = {
    "product": {
        "product_name": "computer",
        "price": 1200
    },
    "store": {
        "store_number": 77,
        "store_city": "London"
    },
    "time": {
        "opening_hours": "8-18",
        "opening_days": "Mon-Fri"
    }
}

# Json to DataFrame
df = pd.json_normalize(json_data)

# DataFrame to Excel
excel_filename = 'json_data_to_excel.xlsx'
df.to_excel(excel_filename, index=False)