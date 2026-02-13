import logging
import time
import traceback

from sqlalchemy.exc import IntegrityError, OperationalError


def insert_bulks(session, instances, max_retries=5, batch_size=200):
    total_inserted = 0
    total_failed = 0

    for i in range(0, len(instances), batch_size):
        batch = instances[i : i + batch_size]

        if _insert_batch(session, batch, max_retries):
            total_inserted += len(batch)
        else:
            total_failed += len(batch)

    logging.info(f"Complete: {total_inserted} inserted, {total_failed} failed")
    return total_inserted, total_failed


def _insert_batch(session, batch, max_retries):
    for attempt in range(max_retries):
        try:
            if session.in_transaction():
                session.rollback()

            session.begin()
            session.bulk_save_objects(batch)
            session.commit()

            logging.info(f"Inserted {len(batch)} records")
            return True

        except IntegrityError:
            logging.warning("Duplicates detected, skipping batch")
            session.rollback()
            return True

        except OperationalError as e:
            if "20018" in str(e):
                logging.error("Error 20018 - data may already be inserted")
                session.close()

                if attempt == max_retries - 1:
                    return False
                time.sleep(10)
            else:
                session.rollback()
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    return False

        except Exception:
            logging.error(traceback.format_exc())
            session.rollback()
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return False

    return False
