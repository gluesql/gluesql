---
title: GlueSQL - Revolutionizing Databases by Unifying Query Interfaces
description: GlueSQL - Revolutionizing Databases by Unifying Query Interfaces
slug: revolutionizing-databases-by-unifying-query-interfaces
authors:
  - name: Taehoon Moon
    title: Creator of GlueSQL
    url: https://github.com/panarch
    image_url: https://github.com/panarch.png
tags: [gluesql, query-interface, database, proposal]
---

# GlueSQL: Revolutionizing Databases by Unifying Query Interfaces

## Introduction
GlueSQL is a versatile database project designed for exceptional portability across a broad range of environments, from embedded systems and servers to web and mobile platforms. The core goal is to support diverse storage environments and manage various data types with a standard SQL approach.

Imagine handling files like CSV, JSONL, and Parquet, or transforming key-value or NoSQL databases such as RocksDB, Redis, and MongoDB into SQL-supporting databases—all feasible with GlueSQL. It can also operate with storages supported in web browsers.

GlueSQL's essential feature is providing a management layer for these diverse storage scenarios without requiring data migration. The broader aim is to facilitate portability of GlueSQL to any environment supporting read or read-write operations. This extends to APIs like GitHub, or messengers like Discord or Slack.

GlueSQL supports both structured and unstructured data and is written in Rust for compatibility with various environments. While portability is its core value, the emphasis is on creating an intuitive, comfortable development environment for easy custom storage implementation.

Ultimately, GlueSQL aims to significantly reduce the cost, time, and complexity of developing new databases. By leveraging GlueSQL for the parser, planner, and execution layer, developers can focus on creating specific storage implementations, leading to a more convenient query interface like SQL for many environments.

## The Problem: Why Reinvent the Database?
Despite the numerous database implementations that currently exist, the emergence of new databases continues. The primary reason behind this trend is our need for databases for a broad spectrum of distinct purposes. For instance, new databases are surfacing that are specifically optimized for Large Language Models (LLMs) like ChatGPT. The range is wide and diverse, encompassing embedded databases, OLAP for data analysis, OLTP databases optimized for online transactions, databases specialized for time-series data processing, and many more.

With such varied requirements, we find ourselves in constant need of fresh databases. However, constructing a database from scratch is a monumental task. It necessitates defining a query interface for handling the database and implementing a corresponding parser. Moreover, a separate execution layer for running operations must be built. Also, the planning layer, which is responsible for devising execution strategies, is a vital aspect of this process. Let's not forget about the critical storage layer that physically reads and stores the data. In a nutshell, there's a daunting amount of work involved in developing a new database.

Given these circumstances, it's understandable why numerous emerging databases resort to high pricing structures—they need immediate revenue to offset continuous development costs.

But the story doesn't end here. Query interfaces like SQL are indeed useful for serious tasks, but they also provide excellent utility for handling simple log files such as CSV, JSONL, Parquet, and even for utilizing REST APIs for various applications. The issue arises when a complex query interface needs to be provided even for these lighter storage requirements—it necessitates a development process almost identical to building a sophisticated database. Implementing an entire parser and execution layer just to add SQL support to an existing service can seem like an excessive burden.

Whether it's a simple storage environment or a serious task, the key lies in the storage layer, which involves the actual reading and storing of data. So, what if developers focused on implementing these storage mechanisms while the remaining parts could be handled using existing libraries? This is the role that GlueSQL aspires to play.


## The Vision of GlueSQL
The GlueSQL project aims to offer a unified query interface for various environments. The goal is to allow anyone to port and use SQL and GlueSQL's proprietary query builder, the AST Builder, in any desired environment. This could range from key-value databases, serious NoSQL databases, log files, and even REST API services. Essentially, if a service supports reading or read-writing data, regardless of the data type, it can readily support a complex query interface via GlueSQL.

Presently, the GlueSQL project itself directly supports a few storage types as reference storages. These include in-memory storage for non-persistent data handling, sled storage, which is a key-value database written in Rust, JSON storage for handling JSON and JSONL files, and a storage that ports SQL to the web browser's IndexedDB. While the GlueSQL Team is primarily developing these, the aim is to allow anyone to create such custom storages for a wide array of purposes, thus enabling them to assemble the database of their choosing.

Imagine using GlueSQL's SQL and AST Builder everywhere, with the simple method of swapping out storages to operate in diverse settings. It could significantly reduce software development costs. Developers wouldn't need to learn the different usage methods for each database. Instead, they could focus solely on implementing business logic using the same interface.

Our vision is to reduce database development costs by 10 times, or even more than 20 times. We aim to gather diverse database creators under the GlueSQL banner, making it the go-to solution for cost-effective database development.


## Benefits to users: the convenience of a unified query interface, ease in software development, and lower costs
From the perspective of the users who engage with databases, there has always been the burden of learning different interfaces to interact with each database. The approach required to work with Redis is different from that necessary for MongoDB. Likewise, handling SQL databases necessitates using SQL. Although SQL databases generally use a common SQL, the SQL they support can considerably vary when examined in detail.

Naturally, there are legitimate reasons for such differences. Each database focuses on different areas, and to cater to specialized functionalities, they incorporate dedicated interface mechanisms. However, not all application development needs to utilize these database-specific core special functionalities.

Let's look at a couple of examples:

Suppose you're developing a back-end application that uses MySQL as the database and Redis for caching. Due to the vast differences in handling SQL databases and Redis, you would have to develop using different methods when storing data.

Here's another scenario:
Imagine you're implementing a data migration pipeline between various databases and log files. Let's say you're transferring Parquet to Redis or MongoDB. In this case, you would need to convert data using different methods for each, all of which would be a cumbersome process.

In both of the above examples, GlueSQL can directly address and solve the issues. It offers the convenience of a uniform query interface to deal with these matters. In certain scenarios, even the construction of a data pipeline can potentially be solved with a single SQL query, thanks to GlueSQL.


## Benefits to developers: a considerable reduction in development costs, making it easier and more cost-effective to create purpose-built databases
If you want to support SQL in the desired environment, using GlueSQL essentially requires you to implement an interface for Storage. There's no need to support all functionalities from the beginning. You can start lightly, choosing and implementing storage features suitable for the environment you want to create. To facilitate this, GlueSQL also provides a library in the form of a test suite to easily validate the storage you've implemented.

Lowering development costs in this way will enable a broader range of developers to support the GlueSQL query interface. As more developers join, a significant synergy can be generated. Designing a query interface from scratch involves a great deal of work, including planning and supporting the interface for different target programming languages.

However, despite all this hard work, it is not easy to attract database users accustomed to different methods.

Consider that the SQL and AST Builder provided by GlueSQL are already securing numerous users. This eliminates the need for efforts to promote a newly planned query interface. Over the years, many new databases have emphasized compatibility with PostgreSQL or MySQL for similar reasons. As GlueSQL places a strong emphasis on portability in its query interface planning, it allows for more flexible configuration according to the desired situation. Through the AST Builder, it also eliminates the cost of porting to different languages.

For many database developers, using GlueSQL can be an optimal choice, as it can save costs and quickly secure users.

Let me mention one more thing: what's convenient for humans... could be applied to AI as well. Rather than making AI write automation code using different databases, providing a common query interface can be much more efficient.


## The Future with GlueSQL
GlueSQL has been and will continue to improve and develop new features to enable portability in various environments. Thanks to the schemaless data support added last year, it is now possible to handle both structured data with schema and unstructured data like JSON simultaneously. This has significantly increased the range of storage environments that can be supported.

One of the key features added last year was the AST Builder. This feature allowed us to escape the confines of SQL and provide an interface for comfortably handling data in the programming languages used for development.

Of course, improving existing features is extremely important, and there are many new features to be added. As a major development plan this year, we aim to develop features to effectively attach GlueSQL to NoSQL databases with their own planners and execution layers. The GlueSQL query planner, currently at a basic level, will see significant changes this year. With the expansion of this planner, not only NoSQL databases but also other SQL databases could be supported without sacrificing performance using GlueSQL.

The synergy that arises from the combination of different databases is a significant bonus in this process.


## The Journey of the GlueSQL Team
The GlueSQL project was first conceived in the fall of 2019, and since then we have been developing it continuously. Personally, I have created various products in a variety of environments, including game development, backend server, and frontend development over the past decade. The experience gained through this process was a major motivation to start the GlueSQL project.

To put it grandly, the inconveniences felt while using different databases in various environments were a major motivation, wouldn't you say?

The start was actually a bit simple. Around 2019, I was mainly doing web front-end development. However, the lack of a structured database for state management and internal data processing made it very uncomfortable, especially since I couldn't use SQL databases and the like. So I started to lightly create an SQL database that could run on a web browser. Also, I wanted to use Rust, but after failing to introduce it at the company I was working for at the time, I decided to use it in my own project.

But as I started developing, my dreams grew significantly. Beyond a SQL database that simply operates on a web browser, I started envisioning a database that fits the name "Glue", one that can easily be ported to various environments, and I continue that journey to this day.

Whether I took the database project too lightly, or because the features I wanted kept increasing, the content to be developed kept expanding. As a result, I ended up investing full time in the GlueSQL project development for over three years. For a year in between, I even juggled full-time software engineering work alongside GlueSQL development. Currently, I'm back to developing the GlueSQL project full time, alongside various part-time contributors.

Now, we're getting very close to the starting point of the picture I wanted to create through GlueSQL, and thankfully, with contributors joining me, I am not alone.


## The Sustainability and Business Aspect of GlueSQL
I believe that what we create through GlueSQL will make a great contribution to the world and make many software engineers happy. This gives me immense strength to continue developing even in difficult situations. However, we cannot accomplish everything with pure passion alone. As much as the GlueSQL project can make a significant contribution, I also see it as holding great business value.

The business strategy of GlueSQL may be somewhat different from other databases. We distribute the project itself as open source under the Apache-2.0 license, so that anyone can use it fully, and we do not consider pricing methods such as restricting features to the storages we support. In fact, if there is any player who can do it better, there's no way to prevent them from taking the GlueSQL project and making it their own.

But we believe that GlueSQL has great potential in this regard. Anyone can participate and they are free to distribute their own storage in the way they want, whether it's open source, private, or commercial. This eliminates the need to create something to replace the GlueSQL project. We aim to prevent the need to recreate the wheel that we provide using GlueSQL.

Moreover, our GlueSQL team seeks to continually expand our group of developers and companies working with us. During this development process, while they can certainly implement everything on their own, there is also no reason not to collaborate with our GlueSQL Team, especially for databases like NoSQL that have their own planners and execution layers. If you have a REST API and want to enhance convenience through SQL support, you can do it yourself or you can collaborate with us.

In addition, for some storages, we can also participate as players in the same position as other custom storage developers. We plan to expand the GlueSQL ecosystem in various ways, such as technical support and storage development.

We are finally ready to provide GlueSQL to users at the production level. We are accelerating the development of GlueSQL. If you are a company interested in storage development like SQL support, or if you resonate with our vision and want to join us, please contact us at taehoon@gluesql.com.


## Conclusion
The continued emergence of new databases is driven by the demand for diverse and specialized databases, such as those optimized for Large Language Models (LLMs) and databases catering to unique requirements, like embedded databases, OLAP, OLTP, and time-series data processing. However, developing a new database from scratch is a significant undertaking, requiring extensive work, which often results in high costs.

GlueSQL presents a solution to this challenge by providing a unified query interface that can be ported across various environments, from key-value databases, NoSQL databases, log files, to REST APIs. It allows anyone to create custom storages, reducing the need for developers to build entirely new databases and to learn different usage methods for each database. Instead, they can focus on implementing their business logic using the same interface.

From a user perspective, GlueSQL offers the convenience of a unified query interface, easing the burden of learning different interfaces for each database. This simplification of interface use can also extend to AI, potentially enhancing the efficiency of AI automation.

GlueSQL's development plan includes significant enhancements to its query planner and aims to enable effective attachment of GlueSQL to NoSQL databases. The synergy of combining different databases is a valuable bonus in this process.

Since its inception in the fall of 2019, the GlueSQL team has continuously developed the project, driven by the desire to mitigate the inconveniences encountered while using different databases in various environments. The journey has been a rewarding one, with the GlueSQL project now at a point where it closely resembles the envisioned product.

GlueSQL, distributed under the Apache-2.0 license, is free for anyone to use and adapt. While the GlueSQL team welcomes collaboration with other developers and companies, they also see significant potential for the project as a business venture. The team is working to expand the GlueSQL ecosystem through a variety of initiatives, including technical support and storage development.

With GlueSQL now ready for production-level use, the team invites companies interested in storage development or those who share their vision to join them in their journey of revolutionizing database development.



