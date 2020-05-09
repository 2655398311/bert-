import pandas as pd

data = pd.read_csv(r'C:\Users\hjfan\Desktop\bozhu1_blog.csv')
total_data = pd.DataFrame()
order_id_list = list(set(data['platform_cid'].values.tolist()))

for ind,order_id in enumerate(order_id_list):
    tmp_data = data[data.platform_cid == order_id]
    tmp_data.to_csv(r"blog_floder\{}.csv".format(str(ind)))
