# The Salesforce integration user is a full System Administrator

**Status:** open, not urgent, security posture
**Found:** 4 Sept 2026, while checking whether we could create two custom
fields.

## What was found

`growth@gushwork.ai` — the user behind `SF_CLIENT_ID` and every Salesforce call
this service makes — has:

```
profile                     : System Administrator
PermissionsCustomizeApplication : true    <- can change the org's schema
PermissionsModifyAllData        : true    <- can read/write/delete any record
PermissionsViewSetup            : true
Opportunity: createable true, updateable true, deletable true
Lead:        createable true, updateable true
Tooling API : reachable
```

Verified by more than metadata: a live `PATCH` returned 204, and two custom
fields were created through the Tooling API.

## What the integration actually needs

| Needs | Has |
|---|---|
| Create + update Lead | ✅ (plus delete on Opportunity, unused) |
| Read Opportunity, update two fields | ✅ (plus create and **delete**, unused) |
| Nothing in Setup | ✅ **Customize Application** |
| No access to unrelated objects | ✅ **Modify All Data** |

So a bug, a leaked credential, or a bad deploy in this service could drop
Opportunities or alter the org's schema. Nothing in the code does that today,
and nothing is proposed that would.

## Suggested fix

A dedicated integration user on a minimal profile, with a permission set
granting exactly: Lead create/read/edit, Opportunity read/edit, and field-level
access to the specific fields listed in `salesforce.js`. No Customize
Application, no Modify All Data, no delete.

Doing this means rotating `SF_CLIENT_ID` / the refresh token, so it needs a
maintenance window and a re-test of the form -> Lead path.

## Related

Creating a custom field via the Tooling API does **not** grant field-level
security. Both fields created on 4 Sept came back `HTTP 201` and were then
invisible to the very user that created them, until `FieldPermissions` rows
were added. Worth knowing for any future field: **create, then grant, then
verify with a real round-trip.**
