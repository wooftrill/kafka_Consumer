import json
import logging
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError, NoSuchTableError
from sqlalchemy.orm import sessionmaker
from HelperUtils.HelperUtils import HelperUtils
from HelperUtils.SQLUtils import SQLUtils

logging.getLogger().setLevel(logging.INFO)

class SQLService:
    def __init__(self):
        # Define your MySQL connection URL and tables
        self.sql_conn = "mysql+pymysql://WTrw:WoofandTrill%4012@localhost:3306/external"
        self.engine = sa.create_engine(self.sql_conn)
        self.order = "tbl_order"
        self.checkout = "tbl_checkout"
        self.inventory = "item"
        self.user_session_cart="tbl_user_session_cart"
        self.metadata = "tbl_payment_metadata"

    def table_exists(self, table_name):
        logging.info("Checking if the table exists...")
        inspect_db = sa.inspect(self.engine)
        is_exist = inspect_db.dialect.has_table(self.engine.connect(), table_name, schema="external")
        return is_exist

    def update_sql_tables(self, message):
        if not all(map(self.table_exists, [self.order, self.checkout, self.inventory, self.metadata])):
            logging.error("One or more required tables do not exist.")
            return False

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                logging.info("strating")
                with self.engine.begin() as conn:
                    session = sessionmaker(bind=conn)()

                    metadata = []
                    query = SQLUtils.check_if_order_id_exist(self.metadata, message["order_id"])
                    query_response = session.execute(sa.text(query))

                    for response in query_response:
                        metadata.append(response)

                    if not metadata :
                        logging.info("this is ")
                        order_query = SQLUtils.update_order_table(
                            self.order,
                            message["payment_status"],
                            message["order_id"],
                            message["session_id"],
                            message["uid"]
                        )
                        print(order_query)
                        conn.execute(sa.text(order_query))

                        status = 0 if message["payment_status"] != 1 else 1
                        checkout_query = SQLUtils.update_checkout_table(
                            self.checkout,
                            status,
                            message["session_id"],
                            message["uid"]
                        )
                        conn.execute(sa.text(checkout_query))

                        user_session_cart_update_query = SQLUtils.update_cart_mapper(
                            self.user_session_cart,
                            status,
                            message["session_id"],
                            message["uid"]
                        )
                        conn.execute(sa.text(user_session_cart_update_query))

                        logging.info("Fetching from Item table")

                        for element in json.loads(message["full_order"]):

                            item_count = []
                            query = SQLUtils.find_item_count(self.inventory, element["item_id"])
                            response = session.execute(sa.text(query))

                            for count in response:
                                item_count.append(count)

                            updated_count = HelperUtils.tupple_to_dict(item_count, ["count"])[0]["count"] - int(element["count"])
                            ldts = HelperUtils.get_timestamp()
                            update_query = SQLUtils.update_inventory(
                                self.inventory,
                                ldts,
                                updated_count,
                                element["item_id"]

                            ).with_for_update()
                            conn.execute(sa.text(update_query))

                        order_ids = (message["order_id"],0)
                        metadata_query = SQLUtils.insert_metadata(self.metadata, order_ids)
                        response = conn.execute(sa.text(metadata_query))
                        if response.__dict__["rowcount"] == 0:
                            raise Exception ("insert failed")
                    session.commit()
                    return True

            except IntegrityError as e:
                # Handle IntegrityError appropriately, log it, and possibly notify administrators
                logging.error(f"IntegrityError: {e}")
                session.rollback()  # Rollback the session on error
                break  # Exit the loop on error

            except NoSuchTableError:
                logging.error("One or more required tables do not exist.")
                break  # Exit the loop on error

            except Exception as e:
                # Log other exceptions, consider retries based on the exception type
                logging.error(f"An error occurred: {e}")
                session.rollback()  # Rollback the session on error
                retry_count += 1

            finally:
                # Close the session explicitly
                session.close()

        return False


#message ={"session_id": "ghsgdhsh7873673hwgdhll-jkj-", "payment_status":1,"uid": "36141bb3a7ccccb7c733e7bff6b697abe84da8c6", "order_id": "00000000-0000-0000-2ef3-33dbdc94f614", "pg_order_id": "ghghghgvbbbbbbbbbbbbbbblllll", "signature": "hjhjrffffffffffff", "net_total": 1785, "full_order": "[{\"cost\": \"595\", \"count\": 1, \"item_id\": \"201816_03_01\", \"net_cost\": 595}, {\"cost\": \"595\", \"count\": 2, \"item_id\": \"201816_03_02\", \"net_cost\": 1190}]"}


#print(SQLService().update_sql_tables(message))
