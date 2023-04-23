---
sidebar_position: 1
---

# Introduction

GlueSQL is not only suitable for use as a conventional database, but one of its key features is the ability for anyone to easily adapt SQL and the AST Builder to their desired file or storage system. This adaptability is achieved through the following topics covered in this section:

## Supported Storages

GlueSQL provides a variety of reference storages out of the box, ranging from simple in-memory storage to key-value databases, log file-based storage like JSON & JSONL, and even Web Storage and IndexedDB supported by web browsers.

## Developing Custom Storages

GlueSQL offers an easy-to-understand and implement interface for custom storage development. By implementing the corresponding interface, developers can have SQL and the AST Builder automatically support their custom storage. 

Verification of custom storage implementation is also straightforward using GlueSQL's test-suite, which allows developers to easily test their implementation and fix any issues found during the process. With a line coverage of nearly 99% in the GlueSQL project's core code, custom storage developers can complete the development and verification process simply by passing all the test-suite cases.

## Exploring the Storages Section

In the Storages section, you will find detailed information about the reference storages currently supported by GlueSQL, as well as guidelines for developing custom storages and what needs to be done to implement them.

Together, these resources make it easy to utilize and adapt GlueSQL for a variety of storage systems.
