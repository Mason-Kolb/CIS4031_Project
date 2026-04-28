from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    
    # insert a new item into item table with i_item_sk = max + 1
    cur.execute(""" INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned)
                    VALUES (( SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item), ?, ?, ?, ?, NULL, ?, ?, ?, ?) """,
                        (new_item.item_id, f"{new_item.start_year}-01-01", new_item.product_name, new_item.brand, new_item.category, new_item.manufact, new_item.current_price, new_item.num_owned))


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    
    # split name into first and last name
    name_parts = new_customer.name.strip().split(" ", 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    # parsing address
    street, city, zip = [part.strip() for part in new_customer.address.split(",", 2)]

    # parsing street into number and name
    street_parts = street.split(" ", 1)
    street_number = street_parts[0]
    street_name = street_parts[1] if len(street_parts) > 1 else ""

    # parsing state and zip
    state_zip_parts = zip.split(" ", 1)
    state = state_zip_parts[0]
    zip_code = state_zip_parts[1] if len(state_zip_parts) > 1 else ""

    # insert into customer address table
    cur.execute(""" INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip)
                    VALUES (( SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address), ?, ?, ?, ?, ?) """,
                        (street_number, street_name, city, state, zip_code))
    
    # get address sk just inserted
    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    address_sk = cur.fetchone()[0]

    # insert into customer table
    cur.execute(""" INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk)
                    VALUES (( SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer), ?, ?, ?, ?, ?) """,
                        (new_customer.customer_id, first_name, last_name, new_customer.email, address_sk))


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    
    # get address foreign key to update address if needed
    cur.execute(""" SELECT c_current_addr_sk FROM customer WHERE c_customer_id=? """, (original_customer_id,))

    address_sk = cur.fetchone()[0]
    if new_customer.address is not None:
        street, city, state_zip = [part.strip() for part in new_customer.address.split(",", 2)]

        street_parts = street.split(" ", 1)
        street_number = street_parts[0]
        street_name = street_parts[1] if len(street_parts) > 1 else ""

        state_zip_parts = state_zip.split(" ", 1)
        state = state_zip_parts[0]
        zip_code = state_zip_parts[1] if len(state_zip_parts) > 1 else ""

        cur.execute("""
            UPDATE customer_address
            SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ?
            WHERE ca_address_sk = ?
        """, (street_number, street_name, city, state, zip_code, address_sk))


    # sql set clauses
    updates = []
    # corresponding params for set clauses, in same order
    params = []
    new_customer_id=None

    # update name if needed
    if new_customer.name is not None:
        name_parts = new_customer.name.strip().split(" ", 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        updates.append("c_first_name=?")
        params.append(first_name)

        updates.append("c_last_name=?")
        params.append(last_name)

    # update email if needed
    if new_customer.email is not None:
        updates.append("c_email_address=?")
        params.append(new_customer.email)
    
    # update customer id if needed
    if new_customer.customer_id is not None:
        updates.append("c_customer_id=?")
        params.append(new_customer.customer_id)
        new_customer_id=new_customer.customer_id
    
    # exe only if there is something to update
    if len(updates) > 0:
        params.append(original_customer_id)
        cur.execute(f""" UPDATE customer SET {', '.join(updates)} WHERE c_customer_id=? """, tuple(params))
    #update other tables so searches can be done with the new customer ID
    if new_customer_id is not None and new_customer_id != original_customer_id:
        cur.execute(""" UPDATE rental SET customer_id=? WHERE customer_id=? """,(new_customer_id, original_customer_id))
        cur.execute(""" UPDATE rental_history SET customer_id=? WHERE customer_id=? """,(new_customer_id, original_customer_id))
        cur.execute(""" UPDATE waitlist SET customer_id=? WHERE customer_id=? """,(new_customer_id, original_customer_id))

def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """

    today = date.today()
    due_date = today + timedelta(days=14)

    # insert rental record into rental table
    cur.execute("""INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)""", (item_id, customer_id, today, due_date))


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """

    # find next place in line
    new_place = line_length(item_id) + 1
    
    # insert into waitlist
    cur.execute(""" INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)""", (item_id, customer_id, new_place))

    return new_place

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    
    # remove person at front of line
    cur.execute(""" DELETE FROM waitlist WHERE item_id=? AND place_in_line=1 """, (item_id,))

    # shift everyone else up by 1
    cur.execute(""" UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id=? """, (item_id,))


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    
    # get rental record
    cur.execute(""" SELECT item_id, customer_id, rental_date, due_date FROM rental WHERE item_id=? AND customer_id=? """, (item_id, customer_id))
    
    rental_info = cur.fetchone()

    if rental_info is None:
        return
    
    # insert into rental_history
    cur.execute(""" INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) VALUES (?, ?, ?, ?, ?) """, (rental_info[0], rental_info[1], rental_info[2], rental_info[3], date.today()))

    # delete from rental
    cur.execute(""" DELETE FROM rental WHERE item_id=? AND customer_id=? """, (item_id, customer_id))


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    
    # update due date by adding 14 days
    cur.execute(""" UPDATE rental SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY) WHERE item_id=? AND customer_id=? """, (item_id, customer_id))


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    #use an empty item if no filters were passed in
    filter_attributes = filter_attributes or Item()

    #query works with duplicate ids by selecting by most recent item id over an older one
    query = """
        SELECT i.i_item_id, i.i_product_name, i.i_brand, i.i_category, i.i_manufact,i.i_current_price,YEAR(i.i_rec_start_date),i.i_num_owned
        FROM item i
        JOIN(
            SELECT i_item_id, MAX(i_rec_start_date) AS max_start_date
            FROM item
            GROUP BY i_item_id
        )latest
          ON i.i_item_id = latest.i_item_id
         AND i.i_rec_start_date = latest.max_start_date
    """

    conds =[]
    params = []

    #use like if pattern matching is turned on
    operator = "LIKE" if use_patterns else "="

    #string filters for item fields
    filters = [
        ("i.i_item_id", filter_attributes.item_id),
        ("i.i_product_name", filter_attributes.product_name),
        ("i.i_brand", filter_attributes.brand),
        ("i.i_category", filter_attributes.category),
        ("i.i_manufact", filter_attributes.manufact)
    ]
    #add each string filter that was actually filled in
    for column, value in filters:
        if value is not None:
            conds.append(f"{column} {operator} ?")
            params.append(value)

    #add number filters if they were set
    if min_price != -1:
        conds.append("i.i_current_price >= ?")
        params.append(min_price)
    if max_price != -1:
        conds.append("i.i_current_price <= ?")
        params.append(max_price)
    if min_start_year != -1:
        conds.append("YEAR(i.i_rec_start_date) >= ?")
        params.append(min_start_year)
    if max_start_year != -1:
        conds.append("YEAR(i.i_rec_start_date) <= ?")
        params.append(max_start_year)

    #only add where if there is something to filter by
    if conds:
        query += " WHERE " + " AND ".join(conds)

    #run the query with all filter values
    cur.execute(query, tuple(params))

    results=[]

    #turn each row into an item object
    for row in cur.fetchall():
        results.append(Item(
            item_id=row[0].strip() if row[0] else None,
            product_name=row[1].strip() if row[1] else None,
            brand=row[2].strip() if row[2] else None,
            category=row[3].strip() if row[3] else None,
            manufact=row[4].strip() if row[4] else None,
            current_price=float(row[5]) if row[5] is not None else -1,
            start_year=int(row[6]) if row[6] is not None else -1,
            num_owned=row[7] if row[7] is not None else -1))
    return results

def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    #use an empty customer if no filters were passed in
    filter_attributes= filter_attributes or Customer()

    #these rebuild full name and full address for searching
    name="TRIM(CONCAT(TRIM(c.c_first_name), ' ', TRIM(c.c_last_name)))"
    address=("TRIM(CONCAT(TRIM(ca.ca_street_number), ' ', TRIM(ca.ca_street_name), " "', ', TRIM(ca.ca_city), ', ', TRIM(ca.ca_state), ' ', TRIM(ca.ca_zip)))")

    #join customer with address so we can search both
    query= f"""
        SELECT c.c_customer_id, c.c_first_name,c.c_last_name,c.c_email_address,ca.ca_street_number, ca.ca_street_name, ca.ca_city, ca.ca_state, ca.ca_zip
        FROM customer c
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk"""
    conds= []
    params=[]
    operator = "LIKE" if use_patterns else "="

    #add each filter only if it was filled in
    if filter_attributes.customer_id is not None:
        conds.append(f"c.c_customer_id {operator} ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.name is not None:
        conds.append(f"{name} {operator} ?")
        params.append(filter_attributes.name)
    if filter_attributes.address is not None:
        conds.append(f"{address} {operator} ?")
        params.append(filter_attributes.address)
    if filter_attributes.email is not None:
        conds.append(f"c.c_email_address {operator} ?")
        params.append(filter_attributes.email)

    #only add where if there is something to filter by
    if conds:
        query+=" WHERE " + " AND ".join(conds)

    #run the query with all filter values
    cur.execute(query, tuple(params))
    results = []

    #rebuild each row into one customer object
    for row in cur.fetchall():
        full_name=f"{row[1].strip() if row[1] else ''} {row[2].strip() if row[2] else ''}".strip()
        full_address = (
            f"{row[4].strip() if row[4] else ''} {row[5].strip() if row[5] else ''}, "
            f"{row[6].strip() if row[6] else ''}, "
            f"{row[7].strip() if row[7] else ''} {row[8].strip() if row[8] else ''}"
        ).strip()

        results.append(Customer(
            customer_id=row[0].strip() if row[0] else None,
            name=full_name,
            address=full_address,
            email=row[3].strip() if row[3] else None,))
    return results

def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    #use an empty rental if no filters were passed in
    filter_attributes = filter_attributes or Rental()

    #base query for rental search
    query="""SELECT item_id, customer_id, rental_date, due_date FROM rental"""
    conds=[]
    params=[]

    #add id filters if they were filled in
    if filter_attributes.item_id is not None:
        conds.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conds.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if min_rental_date is not None:
        conds.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conds.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conds.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conds.append("due_date <= ?")
        params.append(max_due_date)

    #only add where if there is something to filter by
    if conds:
        query += " WHERE " + " AND ".join(conds)

    #run the query with all filter values
    cur.execute(query, tuple(params))

    #turn each row into a rental object
    return [
        Rental(
            item_id=row[0].strip() if row[0] else None,
            customer_id=row[1].strip() if row[1] else None,
            rental_date=str(row[2]) if row[2] is not None else None,
            due_date=str(row[3]) if row[3] is not None else None)
        for row in cur.fetchall()]


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    #use an empty rental history if no filters were passed in
    filter_attributes = filter_attributes or RentalHistory()

    #base query for rental history search
    query = """SELECT item_id, customer_id, rental_date, due_date, return_date FROM rental_history"""
    conds =[]
    params=[]

    #add id filters if they were filled in
    if filter_attributes.item_id is not None:
        conds.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conds.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if min_rental_date is not None:
        conds.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conds.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conds.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conds.append("due_date <= ?")
        params.append(max_due_date)
    if min_return_date is not None:
        conds.append("return_date >= ?")
        params.append(min_return_date)
    if max_return_date is not None:
        conds.append("return_date <= ?")
        params.append(max_return_date)

    #only add where if there is something to filter by
    if conds:
        query += " WHERE " + " AND ".join(conds)

    #run the query with all filter values
    cur.execute(query, tuple(params))

    #turn each row into a rental history object
    return [
        RentalHistory(
            item_id=row[0].strip() if row[0] else None,
            customer_id=row[1].strip() if row[1] else None,
            rental_date=str(row[2]) if row[2] is not None else None,
            due_date=str(row[3]) if row[3] is not None else None,
            return_date=str(row[4]) if row[4] is not None else None)
        for row in cur.fetchall()]


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    #use an empty waitlist object if no filters were passed in
    filter_attributes = filter_attributes or Waitlist()

    #base query for waitlist search
    query ="""SELECT item_id, customer_id, place_in_line FROM waitlist"""
    conds=[]
    params =[]

    #add id filters if they were filled in
    if filter_attributes.item_id is not None:
        conds.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conds.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if min_place_in_line != -1:
        conds.append("place_in_line >= ?")
        params.append(min_place_in_line)
    if max_place_in_line != -1:
        conds.append("place_in_line <= ?")
        params.append(max_place_in_line)

    #only add where if there is something to filter by
    if conds:
        query+= " WHERE " + " AND ".join(conds)

    #run the query with all filter values
    cur.execute(query, tuple(params))

    #turn each row into a waitlist object
    return[
        Waitlist(item_id=row[0].strip() if row[0] else None,customer_id=row[1].strip() if row[1] else None,
            place_in_line=row[2] if row[2] is not None else -1)
        for row in cur.fetchall()]


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """

    #had to make this query match the filter once since some items in the table have the same ID so now this selects the most recent in the table
    cur.execute("""
        SELECT i.i_num_owned
        FROM item i
        JOIN (
            SELECT i_item_id, MAX(i_rec_start_date) AS max_start_date
            FROM item
            GROUP BY i_item_id
        ) latest
          ON i.i_item_id = latest.i_item_id
         AND i.i_rec_start_date = latest.max_start_date
        WHERE i.i_item_id = ?
    """, (item_id,))
    row=cur.fetchone()
    if row is None:
        return -1
    num_owned=row[0]
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id=? ", (item_id,))
    active_rentals=cur.fetchone()[0]
    return num_owned - active_rentals


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """

    # query for place in line
    cur.execute(""" SELECT place_in_line FROM waitlist WHERE item_id=? AND customer_id=? """, (item_id, customer_id))
    
    res = cur.fetchone()
    
    if res is None:
        return -1
    return res[0]


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    
    # counts how many people are on the waitlist for this item
    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id=? ", (item_id,))
    return cur.fetchone()[0]


def save_changes():
    """
    Commits all changes made to the db.
    """
    
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """

    cur.close()
    conn.close()