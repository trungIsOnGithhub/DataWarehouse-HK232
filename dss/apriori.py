# pip install pandas
# pip install numpy
# pip install plotly
# pip install networkx
# pip install matplotlib
import pandas as pd
import numpy as np
import plotly.express as px
from mlxtend.preprocessing import TransactionEncoder

# has to transform to csv before, row are transactions, column are vat_pham
data = pd.read_csv("Market_Basket_Optimisation.csv")

print("num rows: " + data.shape[0] + " num cols: " + data.shape[1])

def display_item_list_by_count(data, color):
    # Gather All vat_pham of Each Transactions into Numpy Array
    transaction = []

    for i in range(0, data.shape[0]):
        for j in range(0, data.shape[1]):
            transaction.append(data.values[i,j])

    transaction = np.array(transaction)
    df = pd.DataFrame(transaction, columns=['vat_pham']) 
    # to be able to perform Group By
    df["lan_xuat_hien"] = 1 
    #  Delete NaN vat_pham from Dataset
    indexNames = df[df['vat_pham'] == "nan"].index

    df.drop(indexNames , inplace=True)
    # new more apporpriate table
    df_table = df.groupby('vat_pham').sum().sort_values("lan_xuat_hien", ascending=False).reset_index()

    df_table.head(10).style.background_gradient(cmap=color)

    return df_table

def display_beautiful(frequent_by_item_table, color):
    df_table["all"] = "all" 
    fig = px.treemap(
        df_table.head(30), path=['all', "vat_pham"], values='lan_xuat_hien',
        color=df_table["lan_xuat_hien"].head(30), hover_data=['vat_pham'],
        color_continuous_scale=color,
    )
    fig.show()

def prep_transform_matrix(frequent_by_item_table, items_to_include):
    te = TransactionEncoder()

    te_ary = te.fit(frequent_by_item_table).transform(frequent_by_item_table)

    dataset = pd.DataFrame(te_ary, columns=te.columns_)

    return dataset.loc[:,items_to_include]

def get_top_item_to_include(frequent_by_item_table, num_item):
    return frequent_by_item_table["items"].head(num_item).values 

def get_support_by_item_set(prepared_dataset, minsupport):
    frequent_itemsets = apriori(prepared_dataset, min_support=minsupport, use_colnames=True)
    frequent_itemsets['so_vat_pham'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
    # printing the frequent itemset
    return frequent_itemsets

def filter_frequent_set(frequent_itemsets, so_vat_pham, support):
    return frequent_itemsets[ (frequent_itemsets['so_vat_pham'] == so_vat_pham) and (frequent_itemsets['support'] >= support) ]

def create_accedent_and_consequent(frequent_itemsets, minthreshold):
    #  set as "Lift" to decide antecedents & consequents dependent our not
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=minthreshold)

    rules["da_mua"] = rules["antecedents"].apply(lambda x: len(x))
    rules["co_the_mua"] = rules["consequents"].apply(lambda x: len(x))

    rules.sort_values("lift",ascending=False)

    print(type(rules))

    return rules

# frequent_by_item_table = display_item_list_by_count(data, 'Greens')
# display_beautiful(frequent_by_item_table, 'Greens')
# prepared_dataset = prep_transform_matrix(frequent_by_item_table, get_top_item_to_include(frequent_by_item_table, 40))
# frequent_itemsets = get_support_by_item_set(prepared_dataset, 0.1)
# filtered_frequent_set = filter_frequent_set(2, 0.05)
# create_accedent_and_consequent(filtered_frequent_set, 1.2)