# Nextdoor Conversions API Tag for Google Tag Manager Server Container

Nextdoor conversion API tag for Google Tag Manager server container allows sending site or app events and parameters directly to Nextdoor server using [Nextdoor API](https://developer.nextdoor.com/reference/conversions-track).

### There are three ways of sending events:

- **Standard** - select one of the standard names.
- **Inherit** from the client - tag will parse sGTM event names and match them to Nextdoor standard events.
- **Custom** - set a custom name.

Nextdoor CAPI tag automatically normalized and hashed with lowercase hex SHA256 format. All user parameters (plain text email, mobile identifier, IP address, and phone number).

The tag supports event deduplication.

## Open Source

The **Nextdoor Tag for GTM Server Side** is developed and maintained by [Stape Team](https://stape.io/) under the Apache 2.0 license.
