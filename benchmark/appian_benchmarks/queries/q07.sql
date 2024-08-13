select t1rp1 as g0, t2rp1 as g1, c.customer_age as g2, c.customer_balance as g3, count(c.customer_name) as p0, sum(c.customer_age) as p1 from CustomerView c left outer join AddressView a on c.customer_id = a.address_customerId left outer join TaxRecordView t on a.address_id = t.taxRecord_addressId left outer join ( select avg(oi.orderItem_weight) as t1rp1, c.customer_id as t1pk from CustomerView c left outer join OrderView o on c.customer_id = o.order_customerId left outer join CreditCardView cc on o.order_creditCardNumber = cc.creditCard_number left outer join OrderItemView oi on o.order_id = oi.orderItem_orderId where creditCard_cvv in (113, 115, 117, 119, 121) group by c.customer_id ) t1 on c.customer_id = t1pk left outer join ( select avg((oi.orderItem_quantity * p.product_price)/(oi.orderItem_weight + oi.orderItem_sku)) as t2rp1, c.customer_id as t2pk from CustomerView c left outer join OrderView o on c.customer_id = o.order_customerId left outer join OrderItemView oi on o.order_id = oi.orderItem_orderId left outer join ProductView p on oi.orderItem_productId = p.product_id left outer join CategoryView ca on p.product_categoryName = ca.category_name where ca.category_name in ('Pet', 'Food', 'Game', 'Software') group by c.customer_id ) t2 on c.customer_id = t2pk where t.taxRecord_bracketThreshold in (22, 24, 27, 29) group by t1rp1, t2rp1, c.customer_age, c.customer_balance order by p0, p1 limit 500;