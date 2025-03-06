import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


df = pd.read_csv("/home/namngo/airflow/data/cleaned_data.csv")



plt.figure(figsize=(10, 6))
tech_counts = df["normalized_job_title"].value_counts().head(10)
sns.barplot(x=tech_counts.values, y=tech_counts.index, hue=tech_counts.index, palette="viridis", legend=False)
plt.title("Xu hướng công nghệ hot")
plt.xlabel("Số lượng công việc")
plt.ylabel("Công nghệ / Vị trí")
plt.savefig("tech_trend.png")

