import itertools

raw_sql_query_output = """actor         | first_name
 actor         | last_update
 actor         | actor_id
 actor         | last_name
 address       | address_id
 address       | phone
 address       | city_id
 address       | address2
 address       | address
 address       | last_update
 address       | postal_code
 address       | district
 category      | category_id
 category      | name
 category      | last_update
 city          | city_id
 city          | country_id
 city          | last_update
 city          | city
 country       | country
 country       | country_id
 country       | last_update
 customer      | last_name
 customer      | email
 customer      | address_id
 customer      | active
 customer      | first_name
 customer      | store_id
 customer      | last_update
 customer      | create_date
 customer      | activebool
 customer      | customer_id
 film          | title
 film          | length
 film          | rental_duration
 film          | rental_rate
 film          | replacement_cost
 film          | rating
 film          | last_update
 film          | film_id
 film          | language_id
 film          | release_year
 film          | description
 film_actor    | last_update
 film_actor    | film_id
 film_actor    | actor_id
 film_category | category_id
 film_category | film_id
 film_category | last_update
 inventory     | film_id
 inventory     | store_id
 inventory     | inventory_id
 inventory     | last_update
 language      | name
 language      | language_id
 language      | last_update
 payment       | customer_id
 payment       | staff_id
 payment       | rental_id
 payment       | amount
 payment       | payment_date
 payment       | payment_id
 rental        | rental_date
 rental        | inventory_id
 rental        | customer_id
 rental        | return_date
 rental        | staff_id
 rental        | rental_id
 rental        | last_update
 staff         | first_name
 staff         | last_name
 staff         | address_id
 staff         | email
 staff         | store_id
 staff         | username
 staff         | password
 staff         | picture
 staff         | staff_id
 staff         | active
 staff         | last_update
 store         | manager_staff_id
 store         | address_id
 store         | store_id
 store         | last_update"""

input_lines = raw_sql_query_output.replace(' ', '').split('\n')
all_possible_indexes = []
lovely_counter = 0

for l in input_lines:
  ct = l.split('|')
  for m in ["btree", "hash"]:
    if not (m == "hash" and ct[1] == "fulltext"):
      all_possible_indexes.append(f"CREATE INDEX iLoveComputerScience{lovely_counter} ON {ct[0]} USING {m}({ct[1]})")
      lovely_counter += 1


column_table_dick = dict()
for l in input_lines:
  ct = l.split('|')
  if (ct[0] not in column_table_dick.keys()):
    column_table_dick[ct[0]] = []
  else:
    column_table_dick[ct[0]].append(ct[1])
for k in column_table_dick.keys():
  tables = column_table_dick[k]
  for L in range(1, len(tables)+1):
      if L <= 3:
        for subset in itertools.combinations(tables, L):
            all_possible_indexes.append(f"CREATE INDEX iLoveComputerScience{lovely_counter} ON {k} USING btree(" + ", ".join(subset) + ")")
            lovely_counter += 1
  
final_check_query = "BEGIN;\n"+";\n".join(all_possible_indexes)+";\n"
final_check_query += """EXPLAIN ANALYZE
SELECT r1.staff_id, p1.payment_date
FROM rental r1, payment p1
WHERE r1.rental_id = p1.rental_id AND
NOT EXISTS (SELECT 1 FROM rental r2, customer c WHERE r2.customer_id =
c.customer_id and active = 1 and r2.last_update > r1.last_update);
"""
final_check_query += """EXPLAIN ANALYZE
SELECT title, release_year
FROM film f1
WHERE f1.rental_rate > (SELECT AVG(f2.rental_rate) FROM film f2 WHERE
f1.release_year = f2.release_year);
"""
final_check_query += """EXPLAIN ANALYZE
SELECT f.title, f.release_year, (SELECT SUM(p.amount) FROM payment p, rental
r1, inventory i1 WHERE p.rental_id = r1.rental_id AND r1.inventory_id =
i1.inventory_id AND i1.film_id = f.film_id)
FROM film f
WHERE NOT EXISTS (SELECT c.first_name, count(*) FROM customer c, rental r2,
inventory i1, film f1, film_actor fa, actor a
WHERE c.customer_id = r2.customer_id AND r2.inventory_id = i1.inventory_id AND
i1.film_id = f1.film_id and f1.rating in ('PG-13','NC-17') AND f1.film_id =
fa.film_id AND f1.film_id = f.film_id AND fa.actor_id = a.actor_id and
a.first_name = c.first_name GROUP BY c.first_name HAVING count(*) >2);
"""
final_check_query += "ROLLBACK;"
with open('out.sql', 'w') as f:
  print(final_check_query, file=f)