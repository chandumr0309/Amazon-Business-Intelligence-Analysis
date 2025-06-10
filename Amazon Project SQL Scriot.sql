USE amazon1;
#14 -- Identify the top 5 most valuable customers using a composite score
WITH customer_metrics AS (
    SELECT
        o.CustomerID,
        COUNT(*) AS order_count,
        SUM(o.SalePrice) AS total_revenue,
        AVG(o.SalePrice) AS avg_order_value
    FROM orders o
    WHERE o.Status = 'Delivered'
    GROUP BY o.CustomerID
),
scored_customers AS (
    SELECT
        CustomerID,
        total_revenue,
        order_count,
        avg_order_value,
        ROUND(
            0.5 * total_revenue +
            0.3 * order_count +
            0.2 * avg_order_value, 2
        ) AS composite_score
    FROM customer_metrics
),
ranked_customers AS (
    SELECT *,
        DENSE_RANK() OVER (ORDER BY composite_score DESC) AS Toprank
    FROM scored_customers
)
SELECT *
FROM ranked_customers
WHERE Toprank <= 5
ORDER BY Toprank, CustomerID;

#15 --  Calculate the month-over-month growth rate in total revenue across the entire dataset.
WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(OrderDate, '%Y-%m') AS month,
        SUM(SalePrice) AS total_revenue
    FROM orders
    WHERE Status = 'Delivered'
    GROUP BY DATE_FORMAT(OrderDate, '%Y-%m')
    ORDER BY month
),
revenue_growth AS (
    SELECT
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY month) AS prev_revenue
    FROM monthly_revenue
)
SELECT
    month,
    total_revenue,
    prev_revenue,
    ROUND(
        IFNULL(((total_revenue - prev_revenue) / prev_revenue) * 100, 0),
        2
    ) AS mom_growth_percent
FROM revenue_growth;

#16 --  Calculate the rolling 3-month average revenue for each product category. 
WITH monthly_revenue AS (
    SELECT 
        ProductCategory,
        DATE_FORMAT(OrderDate, '%Y-%m') AS OrderMonth,
        SUM(SalePrice) AS MonthlyRevenue
    FROM orders
    GROUP BY ProductCategory, OrderMonth
),
ranked_months AS (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY ProductCategory ORDER BY OrderMonth DESC) AS MonthRank
    FROM monthly_revenue
),
last_3_months AS (
    SELECT * FROM ranked_months WHERE MonthRank <= 3
)
SELECT 
    ProductCategory,
    OrderMonth,
    ROUND(
        AVG(MonthlyRevenue) OVER (
            PARTITION BY ProductCategory 
            ORDER BY OrderMonth 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS Rolling3MonthAvgRevenue
FROM last_3_months
ORDER BY ProductCategory, OrderMonth;

#17 --Update the orders table to apply a 15% discount on the `Sale Price` for orders placed by customers who have made at least 10 orders
UPDATE orders
SET SalePrice = SalePrice * 0.85
WHERE CustomerID IN (
    SELECT customer_list.CustomerID
    FROM (
        SELECT CustomerID
        FROM orders
        GROUP BY CustomerID
        HAVING COUNT(*) >= 10
    ) AS customer_list
);

#18 -- Calculate the average number of days between consecutive orders for customers who have placed at least five orders. 
WITH customer_orders AS (
    SELECT 
        CustomerID,
        STR_TO_DATE(OrderDate, '%d/%m/%Y') AS OrderDate
    FROM orders
),
qualified_customers AS (
    SELECT CustomerID
    FROM customer_orders
    GROUP BY CustomerID
    HAVING COUNT(*) >= 5
),
ordered_data AS (
    SELECT 
        o.CustomerID,
        o.OrderDate,
        LAG(o.OrderDate) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS PrevOrderDate
    FROM customer_orders o
    JOIN qualified_customers qc ON o.CustomerID = qc.CustomerID
),
date_diffs AS (
    SELECT 
        CustomerID,
        DATEDIFF(OrderDate, PrevOrderDate) AS DaysBetween
    FROM ordered_data
    WHERE PrevOrderDate IS NOT NULL
)
SELECT 
    ROUND(AVG(DaysBetween), 2) AS AvgDaysBetweenOrders
FROM date_diffs;

#19 -- Identify customers who have generated revenue that is more than 30% higher than the average revenue per customer.
SELECT 
    o.CustomerID,
    SUM(o.SalePrice) AS TotalRevenue
FROM 
    Orders o
GROUP BY 
    o.CustomerID
HAVING 
    SUM(o.SalePrice) > (
        SELECT 1.3 * AVG(CustomerRevenue)
        FROM (
            SELECT 
                CustomerID,
                SUM(SalePrice) AS CustomerRevenue
            FROM Orders
            GROUP BY CustomerID
        ) AS avg_revenue_subquery
    );

#20 --Determine the top 3 product categories that have shown the highest increase in sales over the past year compared to the previous year. 
WITH category_year_sales AS (
    SELECT 
        ProductCategory,
        YEAR(OrderDate) AS OrderYear,
        SUM(SalePrice) AS TotalSales
    FROM Orders
    WHERE OrderDate IS NOT NULL
    GROUP BY ProductCategory, YEAR(OrderDate)
),
category_growth AS (
    SELECT 
        curr.ProductCategory,
        curr.OrderYear AS CurrentYear,
        curr.TotalSales AS CurrentSales,
        prev.TotalSales AS PreviousSales,
        (curr.TotalSales - prev.TotalSales) AS SalesIncrease,
        ROUND(((curr.TotalSales - prev.TotalSales) / prev.TotalSales) * 100, 2) AS PercentGrowth
    FROM category_year_sales curr
    JOIN category_year_sales prev 
        ON curr.ProductCategory = prev.ProductCategory
        AND curr.OrderYear = prev.OrderYear + 1
)
SELECT 
    ProductCategory,
    CurrentYear,
    PreviousSales,
    CurrentSales,
    SalesIncrease,
    PercentGrowth
FROM category_growth
ORDER BY PercentGrowth DESC
LIMIT 3;
