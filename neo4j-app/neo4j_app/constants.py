# TODO: replicate other doc attributes
DOC_CONTENT_LENGTH = "contentLength"
DOC_CONTENT_TYPE = "contentType"
DOC_DIRNAME = "dirname"
DOC_ID = "id"
DOC_EXTRACTION_DATE = "extractionDate"
DOC_NODE = "Document"
DOC_PATH = "path"
DOC_ROOT_ID = "rootId"
DOC_ROOT_REL_LABEL = "HAS_PARENT"
DOC_COLUMNS = {
    DOC_ID,
    DOC_ROOT_ID,
    DOC_DIRNAME,
    DOC_CONTENT_TYPE,
    DOC_CONTENT_LENGTH,
    DOC_EXTRACTION_DATE,
    DOC_PATH,
}

MIGRATION_COMPLETED = "completed"
MIGRATION_LABEL = "label"
MIGRATION_NODE = "Migration"
MIGRATION_STARTED = "started"
MIGRATION_STATUS = "status"
MIGRATION_VERSION = "version"

# TODO: replicate other named entities attributes
NE_APPEARS_IN_DOC = "APPEARS_IN"
NE_ID = "id"
NE_IDS = "mentionIds"
NE_CATEGORY = "category"
NE_DOC_ID = "documentId"
NE_EXTRACTOR = "extractor"
NE_EXTRACTORS = "mentionExtractors"
NE_EXTRACTOR_LANG = "extractorLanguage"
NE_MENTION = "mention"
NE_MENTION_NORM = "mentionNorm"
NE_MENTION_NORM_TEXT_LENGTH = "mentionNormTextLength"
NE_NODE = "NamedEntity"
NE_OFFSETS = "offsets"
NE_COLUMNS = {
    NE_ID,
    NE_CATEGORY,
    NE_DOC_ID,
    NE_EXTRACTOR,
    NE_EXTRACTOR_LANG,
    NE_MENTION,
    NE_MENTION_NORM,
    NE_MENTION_NORM_TEXT_LENGTH,
    NE_OFFSETS,
}
