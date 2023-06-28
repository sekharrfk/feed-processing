import logging


def write_document_to_mongo(mongo_client, domain_id, document, query):
    """
    This function writes the event to domain specific mongo collection
    :return: returns True  - if the document is inserted
                     False - if the document is not inserted
    """
    events_collection = "feed_upload_events_{}".format(domain_id)
    db = mongo_client.get_database("rfk")

    col = None
    # Ensure that the collection exists
    # If not, create it before proceeding any further
    if is_collection_present(db, events_collection):
        col = db.get_collection(name=events_collection)
    else:
        col = create_collection(db, events_collection)

    # Write the document to mongo collection if not present
    if is_document_present(db, events_collection, query):
        logging.info("document already present. skipping the further steps")
        return False

    col.insert_one(document)
    return True


def is_collection_present(db, collection):
    """
    :return: True if collection is present in the database, otherwise False
    """
    all_collections = db.list_collection_names()
    return collection in all_collections


def create_collection(db, collection):
    """
    Creates the collection in database and returns it
    """
    col = db.create_collection(name=collection)
    return col


def is_document_present(db, collection, query):
    """
    Returns true if there is at least one document in the collection satisfying the query
    :param query: python dictionary containing the mongo query
    :return: True if document exists
    """
    col = db.get_collection(name=collection)
    result = col.find_one(filter=query)
    return result is not None
