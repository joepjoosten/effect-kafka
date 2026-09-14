---
"@effect-kafka/native": minor
---

Expose local SASLprep through `SaslPrep.prepare`, `SaslPrep.prepareUnsafe`, and the
`@effect-kafka/native/SaslPrep` subpath. Use the same implementation for SCRAM and
remove `@mongodb-js/saslprep` and its transitive dependencies. Include reproducible
Unicode 3.2 tables and credential-free typed errors. Correct empty mapped strings
and reject the previously omitted U+FFFFE/U+FFFFF noncharacters.
