const BigQuery = require('BigQuery');
const JSON = require('JSON');
const Math = require('Math');
const Object = require('Object');
const getAllEventData = require('getAllEventData');
const getContainerVersion = require('getContainerVersion');
const getCookieValues = require('getCookieValues');
const getRequestHeader = require('getRequestHeader');
const getTimestampMillis = require('getTimestampMillis');
const getType = require('getType');
const logToConsole = require('logToConsole');
const makeInteger = require('makeInteger');
const makeNumber = require('makeNumber');
const makeString = require('makeString');
const parseUrl = require('parseUrl');
const sendHttpRequest = require('sendHttpRequest');
const setCookie = require('setCookie');
const sha256Sync = require('sha256Sync');

/*==============================================================================
==============================================================================*/

const traceId = getRequestHeader('trace-id');

const eventData = getAllEventData();

if (!isConsentGivenOrNotRequired()) {
  return data.gtmOnSuccess();
}

const url = eventData.page_location || getRequestHeader('referer');
if (url && url.lastIndexOf('https://gtm-msr.appspot.com/', 0) === 0) {
  return data.gtmOnSuccess();
}

const commonCookie = eventData.common_cookie || {};

sendTrackRequest(mapEvent(eventData, data));

if (data.useOptimisticScenario) {
  data.gtmOnSuccess();
}

/*==============================================================================
  Vendor related functions
==============================================================================*/

function sendTrackRequest(mappedEvent) {
  const postBody = mappedEvent;
  const postUrl = getPostUrl();

  log({
    Name: 'Nextdoor',
    Type: 'Request',
    TraceId: traceId,
    EventName: mappedEvent.event_name,
    RequestMethod: 'POST',
    RequestUrl: postUrl,
    RequestBody: postBody
  });

  const cookieOptions = {
    domain: 'auto',
    path: '/',
    samesite: 'Lax',
    secure: true,
    'max-age': 31536000, // 1 year
    httpOnly: !!data.useHttpOnlyCookie
  };

  if (mappedEvent.customer.click_id && !data.notSetClickID) {
    setCookie('_ndclid', mappedEvent.customer.click_id, cookieOptions);
  }

  sendHttpRequest(
    postUrl,
    (statusCode, headers, body) => {
      log({
        Name: 'Nextdoor',
        Type: 'Response',
        TraceId: traceId,
        EventName: mappedEvent.event_name,
        ResponseStatusCode: statusCode,
        ResponseHeaders: headers,
        ResponseBody: body
      });

      if (!data.useOptimisticScenario) {
        if (statusCode >= 200 && statusCode < 400) {
          data.gtmOnSuccess();
        } else {
          data.gtmOnFailure();
        }
      }
    },
    {
      headers: {
        authorization: 'Bearer ' + data.accessToken,
        'content-type': 'application/json',
        accept: 'application/json'
      },
      method: 'POST'
    },
    JSON.stringify(postBody)
  );
}

function getPostUrl() {
  return 'https://ads.nextdoor.com/v2/api/conversions/track';
}

function getEventName(eventData, data) {
  if (data.eventType === 'inherit') {
    let eventName = eventData.event_name;

    let gaToEventName = {
      page_view: 'custom_conversion_1',
      'gtm.dom': 'custom_conversion_1',
      purchase: 'purchase',
      'gtm4wp.orderCompletedEEC': 'purchase'
    };

    if (!gaToEventName[eventName]) {
      return 'other';
    }

    return gaToEventName[eventName];
  }

  return data.eventType === 'standard' ? data.eventNameStandard : data.eventNameCustom;
}

function mapEvent(eventData, data) {
  let mappedData = {
    customer: {},
    custom: {}
  };

  mappedData = addServerData(eventData, mappedData);
  mappedData = addUserData(eventData, mappedData);
  mappedData = addCustomData(eventData, mappedData);
  mappedData = addAppData(eventData, mappedData);
  mappedData = hashDataIfNeeded(mappedData);

  return mappedData;
}

function addCustomData(eventData, mappedData) {
  let currencyFromItems = '';
  let valueFromItems = 0;

  if (eventData.items && eventData.items[0]) {
    //mappedData.custom.contents = [];
    mappedData.custom.content_type = 'product';
    currencyFromItems = eventData.items[0].currency;
    mappedData.custom.num_items = eventData.items.length;

    if (!eventData.items[1]) {
      if (eventData.items[0].item_name)
        mappedData.custom.content_name = eventData.items[0].item_name;
      if (eventData.items[0].item_category)
        mappedData.custom.content_category = eventData.items[0].item_category;
      if (eventData.items[0].item_id) mappedData.custom.content_ids = eventData.items[0].item_id;

      if (eventData.items[0].price) {
        mappedData.custom.value = eventData.items[0].quantity
          ? eventData.items[0].quantity * eventData.items[0].price
          : eventData.items[0].price;
      }
    }

    const itemIdKey = data.itemIdKey ? data.itemIdKey : 'item_id';
    eventData.items.forEach((d, i) => {
      let content = {};
      if (d[itemIdKey]) content.id = d[itemIdKey];
      if (d.quantity) content.quantity = d.quantity;
      if (d.delivery_category) content.delivery_category = d.delivery_category;

      if (d.price) {
        content.item_price = makeNumber(d.price);
        valueFromItems += d.quantity ? d.quantity * content.item_price : content.item_price;
      }

      //mappedData.custom.contents.push(content);
    });
  }

  if (eventData.currency) mappedData.custom.currency = eventData.currency;
  else if (currencyFromItems) mappedData.custom.currency = currencyFromItems;

  if (eventData['x-ga-mp1-ev'] && mappedData.custom.currency)
    mappedData.custom.order_value = mappedData.custom.currency + eventData['x-ga-mp1-ev'];
  else if (eventData['x-ga-mp1-tr'] && mappedData.custom.currency)
    mappedData.custom.order_value = mappedData.custom.currency + eventData['x-ga-mp1-tr'];
  else if (eventData.value && mappedData.custom.currency)
    mappedData.custom.order_value = mappedData.custom.currency + eventData.value;

  if (eventData.search_term) mappedData.custom.search_string = eventData.search_term;
  if (eventData.transaction_id) mappedData.custom.order_id = eventData.transaction_id;

  if (mappedData.event_name === 'purchase') {
    let currency = mappedData.custom.currency || 'USD';
    if (!mappedData.custom.order_value)
      mappedData.custom.order_value = currency + (valueFromItems ? valueFromItems : 0);
  }

  if (data.customDataList) {
    data.customDataList.forEach((d) => {
      if (isValidValue(d.value)) {
        mappedData.custom[d.name] = d.value;
      }
    });
  }

  return mappedData;
}

function addAppData(eventData, mappedData) {
  if (data.eventConversionType !== 'app') return mappedData;
  const appData = eventData.app || {};
  mappedData.app = {};

  const appId = data.appId || appData.app_id;
  if (appId) mappedData.app.app_id = appId;

  if (appData.app_tracking_enabled)
    mappedData.app.app_tracking_enabled = appData.app_tracking_enabled;
  else if (eventData.app_tracking_enabled)
    mappedData.app.app_tracking_enabled = eventData.app_tracking_enabled;

  if (appData.platform) mappedData.app.platform = appData.platform;
  else if (eventData.platform) mappedData.app.platform = eventData.platform;

  if (appData.app_version) mappedData.app.app_version = appData.app_version;
  else if (eventData.app_version) mappedData.app.app_version = eventData.app_version;

  if (data.appDataList) {
    data.appDataList.forEach((d) => {
      if (isValidValue(d.value)) {
        mappedData.app[d.name] = d.value;
      }
    });
  }

  return mappedData;
}

function addServerData(eventData, mappedData) {
  mappedData.event_name = getEventName(eventData, data);
  const timestampInMillis = getTimestampMillis();
  mappedData.event_time = convertTimestampToISO(timestampInMillis);
  mappedData.event_time_epoch = makeInteger(timestampInMillis / 1000);
  mappedData.action_source = data.eventConversionType;
  mappedData.data_source_id = data.pixelId;
  mappedData.client_id = data.clientId;
  mappedData.partner_id = 'stapeio_gtm';

  if (data.testEvent) mappedData.testEvent = data.testEvent;
  else if (eventData.testEvent) mappedData.testEvent = eventData.testEvent;

  if (data.eventConversionType === 'website')
    mappedData.action_source_url = eventData.page_location || getRequestHeader('referer');

  const eventId = eventData.event_id || eventData.transaction_id;
  if (eventId) mappedData.event_id = eventId;

  if (data.serverDataList) {
    const restrictedDatUsageNames = [
      'restricted_data_usage',
      'restricted_data_usage_country',
      'restricted_data_usage_state'
    ];
    data.serverDataList.forEach((d) => {
      if (d.name === 'delivery_optimization') return; // Field removed from UI but may still exist in unsynced tags.
      if (isValidValue(d.value)) {
        if (restrictedDatUsageNames.indexOf(d.name) !== -1) {
          mappedData[d.name] = makeInteger(d.value);
        } else {
          mappedData[d.name] = d.value;
        }
      }
    });
  }

  return mappedData;
}

function hashData(value) {
  if (!value) return value;

  const type = getType(value);

  if (value === 'undefined' || value === 'null') return undefined;

  if (type === 'array') {
    return value.map((val) => hashData(val));
  }

  if (type === 'object') {
    return Object.keys(value).reduce((acc, val) => {
      acc[val] = hashData(value[val]);
      return acc;
    }, {});
  }

  if (isHashed(value)) return value;

  return sha256Sync(makeString(value).trim().toLowerCase(), {
    outputEncoding: 'hex'
  });
}

function hashDataIfNeeded(mappedData) {
  const fieldsToHash = [
    'email',
    'phone_number',
    'first_name',
    'last_name',
    'date_of_birth',
    'street_address',
    'city',
    'state',
    'zip_code',
    'country',
    'gender',
    'external_id'
  ];
  for (let key in mappedData.customer) {
    if (fieldsToHash.indexOf(key) !== -1) {
      mappedData.customer[key] = hashData(mappedData.customer[key]);
    }
  }
  return mappedData;
}

function addUserData(eventData, mappedData) {
  const user_data = eventData.user_data || {};

  let address = user_data.address || {};
  const addressType = getType(user_data.address);
  if (addressType === 'object' || addressType === 'array') {
    address = user_data.address[0] || user_data.address;
  }

  const click_id = getClickId();
  if (click_id) mappedData.customer.click_id = click_id;

  mappedData.customer.pixel_id = data.pixelId || eventData.pixel_id;

  if (eventData.email) mappedData.customer.email = eventData.email;
  else if (user_data.email_address) mappedData.customer.email = user_data.email_address;
  else if (user_data.email) mappedData.customer.email = user_data.email;
  else if (user_data.sha256_email_address)
    mappedData.customer.email = user_data.sha256_email_address;

  if (eventData.phone) mappedData.customer.phone_number = eventData.phone;
  else if (user_data.phone_number) mappedData.customer.phone_number = user_data.phone_number;
  else if (user_data.sha256_phone_number)
    mappedData.customer.phone_number = user_data.sha256_phone_number;

  if (eventData.lastName) mappedData.customer.ln = eventData.lastName;
  else if (eventData.LastName) mappedData.customer.ln = eventData.LastName;
  else if (eventData.nameLast) mappedData.customer.ln = eventData.nameLast;
  else if (eventData.last_name) mappedData.customer.ln = eventData.last_name;
  else if (user_data.last_name) mappedData.customer.ln = user_data.last_name;
  else if (address.sha256_last_name) mappedData.customer.ln = address.sha256_last_name;

  if (eventData.firstName) mappedData.customer.fn = eventData.firstName;
  else if (eventData.FirstName) mappedData.customer.fn = eventData.FirstName;
  else if (eventData.nameFirst) mappedData.customer.fn = eventData.nameFirst;
  else if (eventData.first_name) mappedData.customer.fn = eventData.first_name;
  else if (user_data.first_name) mappedData.customer.fn = user_data.first_name;
  else if (address.sha256_first_name) mappedData.customer.fn = address.sha256_first_name;

  if (eventData.date_of_birth) mappedData.customer.date_of_birth = eventData.date_of_birth;

  if (eventData.street_address) mappedData.customer.street_address = eventData.street_address;
  else if (address.street_address) mappedData.customer.street_address = address.street_address;
  else if (address.street) mappedData.customer.street_address = address.street;

  if (eventData.city) mappedData.customer.city = eventData.city;
  else if (address.city) mappedData.customer.city = address.city;

  if (eventData.state) mappedData.customer.state = eventData.state;
  else if (eventData.region) mappedData.customer.state = eventData.region;
  else if (user_data.region) mappedData.customer.state = user_data.region;
  else if (address.region) mappedData.customer.state = address.region;

  if (eventData.zip) mappedData.customer.zip_code = eventData.zip;
  else if (eventData.postal_code) mappedData.customer.zip_code = eventData.postal_code;
  else if (user_data.postal_code) mappedData.customer.zip_code = user_data.postal_code;
  else if (address.postal_code) mappedData.customer.zip_code = address.postal_code;

  if (eventData.countryCode) mappedData.customer.country = eventData.countryCode;
  else if (eventData.country) mappedData.customer.country = eventData.country;
  else if (user_data.country) mappedData.customer.country = user_data.country;
  else if (address.country) mappedData.customer.country = address.country;

  if (eventData.external_id) mappedData.customer.external_id = eventData.external_id;
  else if (eventData.user_id) mappedData.customer.external_id = eventData.user_id;
  else if (eventData.userId) mappedData.customer.external_id = eventData.userId;

  if (eventData.ip_override) {
    mappedData.customer.client_ip_address = eventData.ip_override.split(' ').join('').split(',')[0];
  }

  if (eventData.user_agent) mappedData.customer.client_user_agent = eventData.user_agent;

  if (data.userDataList) {
    data.userDataList.forEach((d) => {
      if (isValidValue(d.value)) {
        mappedData.customer[d.name] = d.value;
      }
    });
  }

  return mappedData;
}

function getClickId() {
  if (eventData.click_id) return eventData.click_id;
  const parsedUrl = parseUrl(url);
  if (parsedUrl && parsedUrl.searchParams.ndclid) {
    return parsedUrl.searchParams.ndclid;
  }
  return getCookieValues('_ndclid')[0] || commonCookie._ndclid;
}

/*==============================================================================
  Helpers
==============================================================================*/

function convertTimestampToISO(timestamp) {
  const leapYear = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const nonLeapYear = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  const secToMs = (s) => s * 1000;
  const minToMs = (m) => m * secToMs(60);
  const hoursToMs = (h) => h * minToMs(60);
  const daysToMs = (d) => d * hoursToMs(24);
  const padStart = (value, length) => {
    let result = makeString(value);
    while (result.length < length) {
      result = '0' + result;
    }
    return result;
  };

  const fourYearsInMs = daysToMs(365 * 4 + 1);
  let year = 1970 + Math.floor(timestamp / fourYearsInMs) * 4;
  timestamp = timestamp % fourYearsInMs;

  while (true) {
    const isLeapYear = year % 4 === 0;
    const nextTimestamp = timestamp - daysToMs(isLeapYear ? 366 : 365);
    if (nextTimestamp < 0) {
      break;
    }
    timestamp = nextTimestamp;
    year = year + 1;
  }

  const daysByMonth = year % 4 === 0 ? leapYear : nonLeapYear;

  let month = 0;
  for (let i = 0; i < daysByMonth.length; i++) {
    const msInThisMonth = daysToMs(daysByMonth[i]);
    if (timestamp > msInThisMonth) {
      timestamp = timestamp - msInThisMonth;
    } else {
      month = i + 1;
      break;
    }
  }
  const date = Math.ceil(timestamp / daysToMs(1));
  timestamp = timestamp - daysToMs(date - 1);
  const hours = Math.floor(timestamp / hoursToMs(1));
  timestamp = timestamp - hoursToMs(hours);
  const minutes = Math.floor(timestamp / minToMs(1));
  timestamp = timestamp - minToMs(minutes);
  const sec = Math.floor(timestamp / secToMs(1));
  timestamp = timestamp - secToMs(sec);

  return (
    year +
    '-' +
    padStart(month, 2) +
    '-' +
    padStart(date, 2) +
    'T' +
    padStart(hours, 2) +
    ':' +
    padStart(minutes, 2) +
    ':' +
    padStart(sec, 2) +
    'Z'
  );
}

function isHashed(value) {
  if (!value) {
    return false;
  }

  return makeString(value).match('^[A-Fa-f0-9]{64}$') !== null;
}

function isValidValue(value) {
  const valueType = getType(value);
  return valueType !== 'null' && valueType !== 'undefined' && value !== '';
}

function isConsentGivenOrNotRequired() {
  if (data.adStorageConsent !== 'required') return true;
  if (eventData.consent_state) return !!eventData.consent_state.ad_storage;
  const xGaGcs = eventData['x-ga-gcs'] || ''; // x-ga-gcs is a string like "G110"
  return xGaGcs[2] === '1';
}

function log(rawDataToLog) {
  const logDestinationsHandlers = {};
  if (determinateIsLoggingEnabled()) logDestinationsHandlers.console = logConsole;
  if (determinateIsLoggingEnabledForBigQuery()) logDestinationsHandlers.bigQuery = logToBigQuery;

  const keyMappings = {
    // No transformation for Console is needed.
    bigQuery: {
      Name: 'tag_name',
      Type: 'type',
      TraceId: 'trace_id',
      EventName: 'event_name',
      RequestMethod: 'request_method',
      RequestUrl: 'request_url',
      RequestBody: 'request_body',
      ResponseStatusCode: 'response_status_code',
      ResponseHeaders: 'response_headers',
      ResponseBody: 'response_body'
    }
  };

  for (const logDestination in logDestinationsHandlers) {
    const handler = logDestinationsHandlers[logDestination];
    if (!handler) continue;

    const mapping = keyMappings[logDestination];
    const dataToLog = mapping ? {} : rawDataToLog;

    if (mapping) {
      for (const key in rawDataToLog) {
        const mappedKey = mapping[key] || key;
        dataToLog[mappedKey] = rawDataToLog[key];
      }
    }

    handler(dataToLog);
  }
}

function logConsole(dataToLog) {
  logToConsole(JSON.stringify(dataToLog));
}

function logToBigQuery(dataToLog) {
  const connectionInfo = {
    projectId: data.logBigQueryProjectId,
    datasetId: data.logBigQueryDatasetId,
    tableId: data.logBigQueryTableId
  };

  dataToLog.timestamp = getTimestampMillis();

  ['request_body', 'response_headers', 'response_body'].forEach((p) => {
    dataToLog[p] = JSON.stringify(dataToLog[p]);
  });

  const bigquery =
    getType(BigQuery) === 'function' ? BigQuery() /* Only during Unit Tests */ : BigQuery;
  bigquery.insert(connectionInfo, [dataToLog], { ignoreUnknownValues: true });
}

function determinateIsLoggingEnabled() {
  const containerVersion = getContainerVersion();
  const isDebug = !!(
    containerVersion &&
    (containerVersion.debugMode || containerVersion.previewMode)
  );

  if (!data.logType) {
    return isDebug;
  }

  if (data.logType === 'no') {
    return false;
  }

  if (data.logType === 'debug') {
    return isDebug;
  }

  return data.logType === 'always';
}

function determinateIsLoggingEnabledForBigQuery() {
  if (data.bigQueryLogType === 'no') return false;
  return data.bigQueryLogType === 'always';
}
