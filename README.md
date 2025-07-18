We are going to create a table `blinkit_city_insights` by integrating data from three tables:
  1. `all_blinkit_category_scraping_stream` (let's call it `stream`)
  2. `blinkit_categories` (let's call it `cat`)
  3. `blinkit_city_map` (let's call it `city_map`)
Steps:
1. Preprocess the `stream` table to convert `created_at` to a date (if it includes time) and to handle inventory changes.
2. We are required to compute `est_qty_sold` for each SKU in each store per day (or per time interval) and then aggregate at the city level (since the final table is per city, not per store).
However, note the final table is at the city level and by date (and sku, etc.). So we must aggregate store-level data to city.
But the estimation of quantity sold is done at the store level and then summed for the city.
Approach for `est_qty_sold`:
  - We have to look at the inventory changes for a given SKU in a given store over consecutive time slots.
  - We are given two cases: 
      Case 1: Inventory decreases -> the decrease is the estimated quantity sold.
      Case 2: Inventory increases -> we need to estimate the sales during that interval (e.g., using average of previous sales in a window).
However, note that the data in `stream` table has multiple records per day per sku per store? Actually, the primary key is {created_at, sku_id, store_id}. So we have multiple time slots in a day.
But the final table is by `date` (without time). So we are aggregating over the entire day.
So we need to:
  - For each store and sku, order the records by `created_at`.
  - For each consecutive pair of records (for the same store and sku), compute the inventory change and assign sales accordingly.
But note: the final table is at the city level and by day. So we will:
  - Compute the estimated sales for each time interval (between two consecutive scrapes) for a store-sku.
  - Then, for each store-sku-day, sum the estimated sales over the day.
  - Then, for each city-sku-day, sum the sales across stores in that city.
However, the problem says: "To estimate quantity sold for a product at a dark store for a date" -> but note the example is between two time slots. So we break down by time slots and then aggregate by day.
How to handle the time slots in SQL?
We can use window functions to get the next inventory value for the same store and sku.
Steps for one store and sku:
  - Order by `created_at` ascending.
  - For each row, compute the next inventory and the next timestamp for the same store and sku.
  - Then, the time interval is between the current row's `created_at` and the next row's `created_at`.
  - The change in inventory: `inventory - next_inventory` (if next_inventory is less, then sales = inventory - next_inventory; if more, then we have restock and we need to estimate).
But note: we are only interested in the day level aggregation. However, the time slots might cross days? The example does (6th Mar 6 pm to 7th Mar 12 am). So we cannot simply group by the day of the current row.
Alternative: we can compute the sales for each interval and then assign the sales to the day in which the interval ended? Or split the sales by day? The problem does not specify.
But note: the final table has a `date` field. According to the schema, it is from `stream.created_at`. However, we are aggregating by day? Actually, the example output table does not specify if it's by day or by timestamp. The schema says `date` and the description says "date of data collection". But note the derived table schema: 
  - `date` is from `all_blinkit_category_scraping_stream.created_at` -> but we are going to group by the date part.
So we will:
  - Extract the date from `created_at` and call it `date`.
  - For each interval, we will attribute the entire sales to the starting day? Or to the ending day? Or split? The problem does not specify.
But note: the example calculation does not split: it says between 6th Mar and 7th Mar, 4 units were sold. And we are to aggregate by city and date. So which date? 6th or 7th? The example doesn't say.
We have to make a decision: we attribute the sales to the day of the starting timestamp? But the interval might cross two days. However, for simplicity, let's attribute the sales to the day of the starting timestamp.
Alternatively, we might aggregate by the day of the starting timestamp and the day of the ending timestamp? But that would require splitting the sales by day, which is complex.
Given the ambiguity, and the fact that the problem states "for a date", I think we are to compute the total sales that occurred during the day. So we can:
  - Consider that the inventory changes that end on a record of a given day are the sales that happened until that record. So for each record, we know the sales since the last record. Then we can assign the sales to the day of the record? 
But note: the sales event happens between two scrapes. We can assign the entire sales to the day of the starting scrape? Or to the day of the ending scrape? 
Alternatively, we can note that the last scrape of the day and the first scrape of the next day might be the only one that crosses days. Since we don't have intraday time information, we can assume that the majority of the sales occur on the day of the starting scrape? 
But to be safe, we can split the sales by the proportion of time in each day? But we don't have the exact time duration? 
Given the complexity and the fact that the problem is an estimation, we can do:
  - For each interval, if the two scrapes are on the same day, then assign the sales to that day.
  - If the interval crosses two days, then we split the sales by the fraction of the interval that falls in each day? But we don't know the exact time of the sale. 
Alternatively, we can avoid this by only considering intervals that are within the same day? But then we might miss the sales that happen overnight.
Another idea: we are aggregating by day. We can compute the sales for each store-sku by taking the first and last record of the day? But that would not account for multiple restocks and sales during the day.
Given the complexity, and the fact that the problem says "you are free to explore alternate approaches", I propose:
  - We will compute the sales per interval (between two consecutive records) and assign the entire sales to the starting day (the day of the first record in the interval).
  - Why? Because the starting day is when we observed the inventory level, and the sale happened after that observation until the next one.
  - This means that the sale that happens from 6th Mar 6pm to 7th Mar 12am is assigned to 6th Mar.
  - Similarly, if we have an interval that starts at 11pm on 6th and ends at 1am on 7th, we assign the entire sale to 6th.
  - This might over-attribute to the starting day and under-attribute to the next day, but without more information, it is a practical approach.
Steps for `est_qty_sold` at the store-sku level per day:
  Step 1: For each store and sku, we order the records by `created_at`.
  Step 2: For each record, we get the next record (using `LEAD` function) for the same store and sku.
  Step 3: Compute the time difference and inventory difference.
  Step 4: If the next record is on the same day or the next day, we assign the sales to the current record's date.
  Step 5: For the inventory change:
          If next_inventory < current_inventory -> sales = current_inventory - next_inventory
          If next_inventory > current_inventory -> restock. Then we need to estimate the sales during this interval.
  How to estimate during restock?
    - We can use the average sales in the previous 3 intervals (for example) for the same store-sku? 
    - But note: we are in the middle of a time series and we are grouping by day. So we need to compute the average sales per interval in the past (for the same store-sku) for a window of time? 
    However, the problem says: "Use historical sales data from previous time slots to estimate the likely sales during this interval."
    We can compute the average sales per interval for the same store-sku in the past 7 days? But note: we are at the interval level, and we don't know how many intervals per day.
    Alternatively, we can compute the average sales per interval for the same store-sku in the last X intervals (say 3 intervals) ignoring restock intervals? 
    But note: if we are at the beginning of the time series, we might not have enough history.
    We'll do:
      - For a restock interval, we look back at the last 3 intervals (for the same store-sku) that were not restock (i.e., sales intervals) and take the average of the sales in those intervals.
      - If there are no such intervals, then we set the sales to 0? Or use a default? The problem doesn't specify. We'll set to 0.
  Step 6: Then, for each store-sku and the current record's date (the starting date of the interval), we add the computed sales (whether actual or estimated) for that interval.
  Step 7: Then, group by store-sku and the date (the starting date) to get the total sales for the day? Actually, we don't need to group by the day because we are assigning each interval to a day and then we will aggregate by day. But note: one store-sku might have multiple intervals on the same day? Yes. So we must sum the sales from all intervals that started on that day for that store-sku.
  Step 8: Then, we join with the city_map to get the city for the store, and then aggregate by city, sku, date, etc.
But note: the final table has one row per (date, sku_id, city_name). So we will aggregate the sales across stores in the same city.
Additional columns:
  - `est_sales_sp` = `est_qty_sold` * `selling_price` -> but note: we have multiple selling_price per sku per store per day? We have to decide which selling_price to use? The schema says:
        est_sales_sp: {est_qty_sold} * {all_blinkit_category_scraping_stream.selling_price}
    But we are aggregating over multiple intervals and multiple stores? 
    Actually, we are computing `est_qty_sold` at the city level. Then we need a representative selling_price for the sku in the city for that day? The derived table schema says:
        sp: mode of {all_blinkit_category_scraping_stream.selling_price}
        mrp: mode of {all_blinkit_category_scraping_stream.mrp}
    And then:
        est_sales_sp = est_qty_sold * sp
        est_sales_mrp = est_qty_sold * mrp
    But note: the `est_qty_sold` is the total quantity sold in the city for that day. So we need to multiply by a single price. We are using the mode (most frequent) selling_price for that sku in the city for that day.
  - Similarly, for `mrp`.
  - Also, we have:
        listed_ds_count: the number of dark stores in the city that listed the sku (i.e., had at least one record for that sku on that day) 
        ds_count: the total number of dark stores in the city (for which we have any data? Actually, we are only considering stores that are in the city_map and that have data in the stream table? But note: the city_map has the store_id and city_name. So for a city, the total dark stores is the count of distinct store_id in the city_map for that city? But wait: the problem says: "exclude stores without a city entry". So we are only including stores that are in the city_map.
        So for a given city and day, the total dark stores (ds_count) is the total distinct store_id in the city_map for that city? But note: the problem says "for the given sku" in the description of listed_ds_count? Actually, the schema says:
            listed_ds_count: ... (the description is cut off, but it says "the given sku")
            ds_count: count of total unique all_blinkit_category_scraping_stream.store_id ... but that is for the entire table? That doesn't make sense.
        Actually, the derived table schema description:
            listed_ds_count: (description incomplete) ... but from the formula in the next column of the schema image, it seems:
                wt_osa: (count of stores where inventory > 0) / ds_count
                wt_osa_ls: (count of stores where inventory > 0) / listed_ds_count
            And the description of wt_osa: "dark stores where the given sku is in stock out of the total dark stores"
            wt_osa_ls: "dark stores where the given sku is in stock out of the total listed dark stores"
        So:
            listed_ds_count: number of dark stores in the city that listed the sku (i.e., had the sku at least once during the day? regardless of inventory)
            ds_count: total number of dark stores in the city (from the city_map? for the entire city, regardless of the sku)
        How to compute:
            ds_count: for a city, it is the count of distinct store_id in the city_map for that city. But note: the city_map table has one row per store_id. So for each city, we can precompute the total number of stores.
        However, the ds_count is the same for every sku and every day in the same city? So we can precompute it.
        But note: the derived table is at the city level per day per sku. So for each row, we need the total number of dark stores in the city (which is constant for the city).
        Similarly, listed_ds_count for a given sku and city and day: the count of distinct store_id in the stream table for that city, sku, and day.
        Then:
            wt_osa = (number of stores in the city for that sku and day that had at least one record with inventory > 0) / ds_count
            wt_osa_ls = (number of stores in the city for that sku and day that had at least one record with inventory > 0) / listed_ds_count
        But note: the description of wt_osa says: "dark stores where the given sku is in stock out of the total dark stores". So we are counting a store as in stock for the sku on that day if at any time during the day the inventory was >0? Or at the end of the day? Or at the beginning? The problem doesn't specify.
        We'll define: a store is considered "in stock" for a sku on a day if there is at least one record during the day for that store-sku with inventory > 0.
  - Also, we need to compute:
        mrp: the mode of mrp for that sku in the city for the day.
        sp: the mode of selling_price for that sku in the city for the day.
        Then discount = (mrp - sp) / mrp.
