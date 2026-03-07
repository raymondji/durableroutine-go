# Reflection vs Kind()

Analyzed reflection as alternative to explicit `Kind()` methods. Recommendation: **Keep explicit `Kind()`.** Renaming a struct silently breaks routing keys for in-flight routines (killer issue for durable workflows). One-line-per-type cost is low vs risks.
