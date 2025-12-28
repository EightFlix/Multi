# database/ia_filterdb.py

import logging
from struct import pack
import re
import base64
from datetime import datetime
from hydrogram.file_id import FileId
from pymongo import MongoClient, TEXT
from pymongo.errors import DuplicateKeyError, OperationFailure
from bson import ObjectId
from info import (
    USE_CAPTION_FILTER, 
    FILES_DATABASE_URL, 
    SECOND_FILES_DATABASE_URL, 
    DATABASE_NAME, 
    MAX_BTN
)

logger = logging.getLogger(__name__)

# ==================== Database Connection ====================

client = MongoClient(
    FILES_DATABASE_URL,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=45000
)
db = client[DATABASE_NAME]

# Three Collections for Smart Organization
primary_collection = db['primary_files']
clouds_collection = db['cloud_files']
archive_collection = db['archive_files']

# ==================== Create Indexes ====================

def create_indexes():
    """Create indexes for all file collections"""
    try:
        for col in [primary_collection, clouds_collection, archive_collection]:
            # Text index for search
            col.create_index([("file_name", TEXT), ("caption", TEXT)])
            # Other indexes
            col.create_index([("file_size", 1)])
            col.create_index([("added_date", -1)])
            col.create_index([("file_type", 1)])
        
        logger.info("✅ Indexes created for all collections")
    except OperationFailure as e:
        if 'quota' in str(e).lower():
            logger.error(f'Database quota exceeded: {e}')
        else:
            logger.exception(e)

create_indexes()

# Second Database (if provided)
if SECOND_FILES_DATABASE_URL:
    second_client = MongoClient(SECOND_FILES_DATABASE_URL)
    second_db = second_client[DATABASE_NAME]
    second_primary = second_db['primary_files']
    second_clouds = second_db['cloud_files']
    second_archive = second_db['archive_files']
    
    for col in [second_primary, second_clouds, second_archive]:
        col.create_index([("file_name", TEXT), ("caption", TEXT)])
    
    logger.info("✅ Second database connected")

# ==================== Helper Functions ====================

def get_collection_by_name(collection_name):
    """Get collection object by name"""
    collections = {
        'primary': primary_collection,
        'clouds': clouds_collection,
        'archive': archive_collection
    }
    return collections.get(collection_name, primary_collection)


def get_all_collections():
    """Return list of all collection objects"""
    cols = [primary_collection, clouds_collection, archive_collection]
    if SECOND_FILES_DATABASE_URL:
        cols.extend([second_primary, second_clouds, second_archive])
    return cols


def db_count_documents(collection_name='primary'):
    """Count documents in specific collection"""
    col = get_collection_by_name(collection_name)
    return col.count_documents({})


def get_all_counts():
    """Get counts from all collections"""
    counts = {
        'primary': primary_collection.count_documents({}),
        'clouds': clouds_collection.count_documents({}),
        'archive': archive_collection.count_documents({})
    }
    
    if SECOND_FILES_DATABASE_URL:
        counts['second_primary'] = second_primary.count_documents({})
        counts['second_clouds'] = second_clouds.count_documents({})
        counts['second_archive'] = second_archive.count_documents({})
    
    return counts

# ==================== File Operations ====================

async def save_file(media, collection_name='primary'):
    """Save file in specific collection
    
    Args:
        media: Media object
        collection_name: 'primary', 'clouds', or 'archive'
    
    Returns:
        tuple: (status, collection_name) where status is 'suc', 'dup', or 'err'
    """
    file_id = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.file_name))
    file_caption = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.caption))
    
    document = {
        '_id': file_id,
        'file_name': file_name,
        'file_size': media.file_size,
        'caption': file_caption,
        'collection_type': collection_name,
        'file_type': getattr(media, 'mime_type', 'unknown'),
        'added_date': datetime.now()
    }
    
    target_col = get_collection_by_name(collection_name)
    
    try:
        target_col.insert_one(document)
        logger.info(f'✅ Saved to {collection_name} - {file_name}')
        return 'suc', collection_name
    except DuplicateKeyError:
        logger.warning(f'Already Saved in {collection_name} - {file_name}')
        return 'dup', collection_name
    except OperationFailure:
        if SECOND_FILES_DATABASE_URL:
            try:
                second_col = {
                    'primary': second_primary,
                    'clouds': second_clouds,
                    'archive': second_archive
                }.get(collection_name, second_primary)
                
                second_col.insert_one(document)
                logger.info(f'✅ Saved to 2nd db ({collection_name}) - {file_name}')
                return 'suc', f'second_{collection_name}'
            except DuplicateKeyError:
                logger.warning(f'Already Saved in 2nd db - {file_name}')
                return 'dup', f'second_{collection_name}'
        else:
            logger.error(f'Database is full, add SECOND_FILES_DATABASE_URL')
            return 'err', collection_name


async def get_search_results(query, collection_name=None, max_results=MAX_BTN, offset=0, lang=None):
    """Search files in specific or all collections
    
    Args:
        query: Search query
        collection_name: 'primary', 'clouds', 'archive', or None for all
        max_results: Maximum results to return
        offset: Pagination offset
        lang: Language filter (optional)
    
    Returns:
        tuple: (files, next_offset, total_results, counts_dict)
    """
    query = str(query).strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query

    if USE_CAPTION_FILTER:
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter_query = {'file_name': regex}

    # Search in specific collection
    if collection_name:
        target_col = get_collection_by_name(collection_name)
        cursor = target_col.find(filter_query)
        results = [doc for doc in cursor]
        
        # Add to second db if exists
        if SECOND_FILES_DATABASE_URL:
            second_col = {
                'primary': second_primary,
                'clouds': second_clouds,
                'archive': second_archive
            }.get(collection_name)
            
            if second_col:
                cursor2 = second_col.find(filter_query)
                results.extend([doc for doc in cursor2])
        
        # Add source collection to results
        for doc in results:
            doc['source_collection'] = collection_name
    
    # Search in all collections
    else:
        results = []
        counts = {}
        
        for col_name in ['primary', 'clouds', 'archive']:
            col = get_collection_by_name(col_name)
            cursor = col.find(filter_query)
            col_results = [doc for doc in cursor]
            
            # Add source collection
            for doc in col_results:
                doc['source_collection'] = col_name
            
            results.extend(col_results)
            counts[col_name] = len(col_results)
            
            # Second database
            if SECOND_FILES_DATABASE_URL:
                second_col = {
                    'primary': second_primary,
                    'clouds': second_clouds,
                    'archive': second_archive
                }.get(col_name)
                
                cursor2 = second_col.find(filter_query)
                second_results = [doc for doc in cursor2]
                
                for doc in second_results:
                    doc['source_collection'] = col_name
                
                results.extend(second_results)
                counts[col_name] += len(second_results)

    # Language filter
    if lang:
        lang_files = [file for file in results if lang in file['file_name'].lower()]
        files = lang_files[offset:][:max_results]
        total_results = len(lang_files)
    else:
        total_results = len(results)
        files = results[offset:][:max_results]

    next_offset = offset + max_results
    if next_offset >= total_results:
        next_offset = ''
    
    # Return counts if searching all collections
    if collection_name:
        return files, next_offset, total_results
    else:
        return files, next_offset, total_results, counts


async def get_search_counts(query):
    """Get count of search results in each collection
    
    Returns:
        dict: {'primary': count, 'clouds': count, 'archive': count}
    """
    query = str(query).strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query

    if USE_CAPTION_FILTER:
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter_query = {'file_name': regex}
    
    counts = {}
    for col_name in ['primary', 'clouds', 'archive']:
        col = get_collection_by_name(col_name)
        counts[col_name] = col.count_documents(filter_query)
        
        # Add second db count
        if SECOND_FILES_DATABASE_URL:
            second_col = {
                'primary': second_primary,
                'clouds': second_clouds,
                'archive': second_archive
            }.get(col_name)
            counts[col_name] += second_col.count_documents(filter_query)
    
    return counts


async def delete_files(query, collection_name=None):
    """Delete files matching query
    
    Args:
        query: Search query
        collection_name: Specific collection or None for all
    
    Returns:
        int: Total deleted count
    """
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query
        
    filter_query = {'file_name': regex}
    
    total_deleted = 0
    
    if collection_name:
        # Delete from specific collection
        col = get_collection_by_name(collection_name)
        result = col.delete_many(filter_query)
        total_deleted = result.deleted_count
        
        if SECOND_FILES_DATABASE_URL:
            second_col = {
                'primary': second_primary,
                'clouds': second_clouds,
                'archive': second_archive
            }.get(collection_name)
            result2 = second_col.delete_many(filter_query)
            total_deleted += result2.deleted_count
    else:
        # Delete from all collections
        for col in [primary_collection, clouds_collection, archive_collection]:
            result = col.delete_many(filter_query)
            total_deleted += result.deleted_count
        
        if SECOND_FILES_DATABASE_URL:
            for col in [second_primary, second_clouds, second_archive]:
                result = col.delete_many(filter_query)
                total_deleted += result.deleted_count
    
    return total_deleted


async def get_file_details(query):
    """Get file details by ID"""
    # Search in all collections
    for col in [primary_collection, clouds_collection, archive_collection]:
        file_details = col.find_one({'_id': query})
        if file_details:
            return file_details
    
    # Check second database
    if SECOND_FILES_DATABASE_URL:
        for col in [second_primary, second_clouds, second_archive]:
            file_details = col.find_one({'_id': query})
            if file_details:
                return file_details
    
    return None


async def move_file(file_id, from_collection, to_collection):
    """Move file from one collection to another
    
    Returns:
        tuple: (success, message)
    """
    from_col = get_collection_by_name(from_collection)
    to_col = get_collection_by_name(to_collection)
    
    # Find file
    file = from_col.find_one({'_id': file_id})
    
    if not file:
        return False, "File not found in source collection"
    
    # Update collection type
    file['collection_type'] = to_collection
    file['moved_date'] = datetime.now()
    
    try:
        # Insert in new collection
        to_col.insert_one(file)
        # Delete from old collection
        from_col.delete_one({'_id': file_id})
        
        logger.info(f"Moved file from {from_collection} to {to_collection}")
        return True, f"File moved successfully"
    except Exception as e:
        logger.error(f"Error moving file: {e}")
        return False, str(e)


async def copy_file(file_id, from_collection, to_collection):
    """Copy file to another collection
    
    Returns:
        tuple: (success, new_id, message)
    """
    from_col = get_collection_by_name(from_collection)
    to_col = get_collection_by_name(to_collection)
    
    # Find file
    file = from_col.find_one({'_id': file_id})
    
    if not file:
        return False, None, "File not found"
    
    # Create copy
    file_copy = file.copy()
    # Keep same _id to maintain file_id reference
    file_copy['collection_type'] = to_collection
    file_copy['copied_date'] = datetime.now()
    
    try:
        # Check if already exists in target
        existing = to_col.find_one({'_id': file_id})
        if existing:
            return False, None, "File already exists in target collection"
        
        to_col.insert_one(file_copy)
        logger.info(f"Copied file from {from_collection} to {to_collection}")
        return True, file_id, "File copied successfully"
    except Exception as e:
        logger.error(f"Error copying file: {e}")
        return False, None, str(e)


async def bulk_move_files(query, from_collection, to_collection):
    """Move multiple files matching query
    
    Returns:
        int: Number of files moved
    """
    from_col = get_collection_by_name(from_collection)
    to_col = get_collection_by_name(to_collection)
    
    # Create regex
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query
    
    filter_query = {'file_name': regex}
    
    # Find all matching files
    files = list(from_col.find(filter_query))
    
    moved_count = 0
    for file in files:
        file['collection_type'] = to_collection
        file['moved_date'] = datetime.now()
        
        try:
            to_col.insert_one(file)
            from_col.delete_one({'_id': file['_id']})
            moved_count += 1
        except DuplicateKeyError:
            # Skip if already exists
            continue
    
    logger.info(f"Bulk moved {moved_count} files from {from_collection} to {to_collection}")
    return moved_count


# ==================== File ID Encoding ====================

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    return file_id
