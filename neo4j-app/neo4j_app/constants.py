PROJECT_REGISTRY_DB = "datashare-project-registry"

NEO4J_CSV_COL = "node_col"

# TODO: replicate other doc attributes
DOC_NODE = "Document"
DOC_CONTENT_LENGTH = "contentLength"
DOC_CONTENT_TYPE = "contentType"
DOC_CREATED_AT = "createdAt"
DOC_DIRNAME = "dirname"
DOC_ID = "id"
DOC_ID_CSV = f"ID({DOC_NODE})"
DOC_EXTRACTION_DATE = "extractionDate"
DOC_EXTRACTION_LEVEL = "extractionLevel"
DOC_METADATA = "metadata"
DOC_MODIFIED_AT = "modifiedAt"
DOC_PATH = "path"
DOC_URL_SUFFIX = "urlSuffix"
DOC_ROOT_ID = "rootDocument"
DOC_ROOT_TYPE = "HAS_PARENT"
DOC_TITLE = "title"
DOC_COLUMNS = {
    DOC_ID: {NEO4J_CSV_COL: DOC_ID_CSV},
    DOC_DIRNAME: {},
    DOC_CONTENT_TYPE: {},
    DOC_CONTENT_LENGTH: {NEO4J_CSV_COL: "LONG"},
    DOC_EXTRACTION_DATE: {NEO4J_CSV_COL: "DATETIME"},
    DOC_EXTRACTION_LEVEL: {NEO4J_CSV_COL: "LONG"},
    DOC_METADATA: {},
    DOC_PATH: {},
    DOC_TITLE: {},
    DOC_URL_SUFFIX: {},
}

DOC_ES_SOURCES = list(DOC_COLUMNS) + ["join", DOC_ROOT_ID]

# Order matters here, we're taking the cdterms create in priority to be consistent
# with datashare-api which sets the creationDate as from tika_metadata_dcterms_created
# we fall back to other metadata if this one is missing
DOC_CREATED_AT_META = [
    "tika_metadata_dcterms_created_iso8601",
    "tika_metadata_creation_date_iso8601",
    "tika_metadata_date_iso8601",
]
DOC_MODIFIED_AT_META = [
    "tika_metadata_dcterms_modified_iso8601",
    "tika_metadata_last_modified_iso8601",
    "tika_metadata_modified_iso8601",
    "tika_metadata_last_save_date_iso8601",
    "tika_metadata_pdf_docinfo_modified_iso8601",
    "tika_metadata_date_iso8601",
]

PROJECT_RUNS_MIGRATION = "_RUNS"
PROJECT_NAME = "name"
PROJECT_NODE = "_Project"

# TODO: check that it the name retained in https://github.com/ICIJ/datashare/pull/1180
EMAIL_CATEGORY = "EMAIL"
EMAIL_DOMAIN = "emailDomain"
EMAIL_HEADER = "emailHeaderField"
EMAIL_RECEIVED_TYPE = "RECEIVED"
EMAIL_SENT_TYPE = "SENT"
EMAIL_USER = "emailUser"
# TODO: check the naming here, we use "fields" here since the RFC specification
#  https://www.rfc-editor.org/rfc/rfc2822 refers to this kind of header as fields, this
#  is not very ubiquitous nor end user friendly. FTM and other knowledge base don't
#  seem to propose a naming for these fields. "role" might be friendly
EMAIL_REL_HEADER_FIELDS = "fields"

EMAIL_REL_COLS = {
    EMAIL_REL_HEADER_FIELDS: {NEO4J_CSV_COL: "STRING[]"},
}

# TODO: check that this list is exhaustive, we know it isn't !!!
SENT_EMAIL_HEADERS = {"tika_metadata_message_from", "tika_metadata_dc_creator"}
# TODO: check that this list is exhaustive, we know it isn't !!!
RECEIVED_EMAIL_HEADERS = {
    "tika_metadata_message_bcc",
    "tika_metadata_message_cc",
    "tika_metadata_message_to",
}

MIGRATION_COMPLETED = "completed"
MIGRATION_LABEL = "label"
MIGRATION_NODE = "_Migration"
MIGRATION_PROJECT = "project"
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
NE_MENTION_COUNT = "mentionCount"
NE_MENTION_NORM = "mentionNorm"
NE_MENTION_NORM_TEXT_LENGTH = "mentionNormTextLength"
NE_METADATA = "metadata"

NE_NODE = "NamedEntity"
NE_OFFSETS = "offsets"
NE_COLUMNS = {
    NE_ID: {},
    NE_CATEGORY: {},
    NE_DOC_ID: {},
    NE_EXTRACTOR: {},
    NE_EXTRACTOR_LANG: {},
    NE_MENTION: {},
    NE_MENTION_NORM: {},
    NE_MENTION_NORM_TEXT_LENGTH: {NEO4J_CSV_COL: "INT"},
    # TODO: this shouldn't be imported in admin imports...
    NE_METADATA: {},
    NE_OFFSETS: {NEO4J_CSV_COL: "LONG[]"},
}
NE_ES_SOURCES = list(NE_COLUMNS) + ["join", NE_DOC_ID]
NE_APPEARS_IN_DOC_COLS = {
    NE_EXTRACTORS: {NEO4J_CSV_COL: "STRING[]"},
    NE_EXTRACTOR_LANG: {},
    NE_IDS: {NEO4J_CSV_COL: "STRING[]"},
    NE_OFFSETS: {NEO4J_CSV_COL: "LONG[]"},
}
NEO4J_ARRAY_SPLIT_CHAR = "|"
NEO4J_CSV_END_ID = ":END_ID"
NEO4J_CSV_ID = ":ID"
NEO4J_CSV_LABEL = ":LABEL"
NEO4J_CSV_START_ID = ":START_ID"
NEO4J_CSV_TYPE = ":TYPE"

STATS_NODE = "_ProjectStatistics"
STATS_N_DOCS = "nDocuments"
STATS_N_ENTS = "nEntities"
STATS_ID = "id"

TASK_NODE = "_Task"
TASK_COMPLETED_AT = "completedAt"
TASK_CREATED_AT = "createdAt"
TASK_ID = "id"
TASK_INPUTS = "inputs"
TASK_PROGRESS = "progress"
TASK_RETRIES = "retries"
TASK_TYPE = "type"

TASK_LOCK_NODE = "_TaskLock"
TASK_LOCK_TASK_ID = "taskId"
TASK_LOCK_WORKER_ID = "workerId"

TASK_ERROR_NODE = "_TaskError"
TASK_ERROR_DETAIL = "detail"
TASK_ERROR_ID = "id"
TASK_ERROR_OCCURRED_AT = "occurredAt"
TASK_ERROR_TITLE = "title"

TASK_ERROR_OCCURRED_TYPE = "_OCCURRED_DURING"

TASK_RESULT_NODE = "_TaskResult"
TASK_HAS_RESULT_TYPE = "_HAS_RESULT"
TASK_RESULT_RESULT = "result"
