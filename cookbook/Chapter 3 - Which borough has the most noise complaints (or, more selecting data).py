# %%
import pandas as pd
import matplotlib.pyplot as plt
import polars as pl

# Make the graphs a bit prettier, and bigger
plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 5)

# This is necessary to show lots of columns in pandas 0.12.
# Not necessary in pandas 0.13.
pd.set_option("display.width", 5000)
pd.set_option("display.max_columns", 60)

# %%
# Let's continue with our NYC 311 service requests example.
# because of mixed types we specify dtype to prevent any errors
#complaints = pd.read_csv("C:/Users/nick/Projects/pandas_to_polars_cookbook/data/311-service-requests.csv", dtype="unicode")

# %%
# TODO: rewrite the above using the polars library (you might have to import it above) and call the data frame pl_complaints
pl_complaints = pl.read_csv("C:/Users/nick/Projects/pandas_to_polars_cookbook/data/311-service-requests.csv", infer_schema_length=0)
# %%
# 3.1 Selecting only noise complaints
# I'd like to know which borough has the most noise complaints. First, we'll take a look at the data to see what it looks like:
print(pl_complaints[:5])

# %%
# TODO: rewrite the above in polars

# %%
# To get the noise complaints, we need to find the rows where the "Complaint Type" column is "Noise - Street/Sidewalk".
pl_noise_complaints = pl_complaints.filter(
    pl.col("Complaint Type") == "Noise - Street/Sidewalk")
print(pl_noise_complaints[:3])

# %%
# TODO: rewrite the above in polars


# %%
# Combining more than one condition
is_noise = pl.col("Complaint Type") == "Noise - Street/Sidewalk"
in_brooklyn = pl.col("Borough") == "BROOKLYN"
pl_noise_in_brooklyn = pl_complaints.filter(is_noise & in_brooklyn)
print(pl_noise_in_brooklyn.head(5))

# %%
# TODO: rewrite the above using the Polars library. In polars these conditions are called Expressions.
# Check out the Polars documentation for more info.


# %%
# If we just wanted a few columns:
#complaints[is_noise & in_brooklyn][
#   ["Complaint Type", "Borough", "Created Date", "Descriptor"]
#][:10]

# %%
# TODO: rewrite the above using the polars library
print(pl_noise_in_brooklyn.select(["Complaint Type", "Borough", "Created Date", "Descriptor"]).head(10))

# %%
# 3.3 So, which borough has the most noise complaints?
#is_noise = complaints["Complaint Type"] == "Noise - Street/Sidewalk"

# %%
# TODO: rewrite the above using the polars library
noise_complaints = pl_complaints.filter(is_noise)

# Perform value counts on the "Borough" column
pl_borough_counts = noise_complaints.group_by("Borough").agg(
    pl.col("Borough").count().alias("count")).sort("count", descending=True)
print(pl_borough_counts)

# %%
# What if we wanted to divide by the total number of complaints?
#noise_complaint_counts = noise_complaints["Borough"].value_counts()
#complaint_counts = complaints["Borough"].value_counts()

#noise_complaint_counts / complaint_counts.astype(float)

# %%
# TODO: rewrite the above using the polars library
noise_complaint_counts = noise_complaints.group_by("Borough").agg(
    pl.col("Borough").count().alias("noise_count"))
print(noise_complaint_counts)
complaint_counts = pl_complaints.group_by("Borough").agg(
    pl.col("Borough").count().alias("total_count"))
print(complaint_counts)
counts_joined = noise_complaint_counts.join(complaint_counts, on="Borough", how="inner")

counts_joined = counts_joined.with_columns(
    (pl.col("noise_count") / pl.col("total_count")).alias("noise_complaint_ratio"))
print(counts_joined)
# %%
# Plot the results
#(noise_complaint_counts / complaint_counts.astype(float)).plot(kind="bar")
pd_counts_joined = counts_joined.to_pandas()
pd_counts_joined.set_index("Borough", inplace=True)
pd_counts_joined["noise_complaint_ratio"].plot(kind="bar", x="Borough")
plt.title("Noise Complaints by Borough (Normalized)")
plt.xlabel("Borough")
plt.ylabel("Ratio of Noise Complaints to Total Complaints")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%
# TODO: rewrite the above using the polars library. NB: polars' plotting method is sometimes unstable. You might need to use seaborn or matplotlib for plotting.
