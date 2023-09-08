---
blogpost: true
date: 2023-Aug-25
author: ryan
tags: releases
excerpt: 2
description: Embedded in Python for accessible Real-Time AI
---

# Introducing the New Kaskada

We started Kaskada with the goal of simplifying the real-time AI/ML lifecycle, and in the past year AI has exploded in usefulness and accessibility. Generative models and Large Language Models (LLMs) have revolutionized how we approach AI. Their accessibility and incredible capabilities have made AI more valuable than it has ever been and democratized the practice of AI.

Still, a challenge remains: building and managing real-time AI applications.

## The Challenge of using Real-Time Data in AI Applications

Real-time data for AI Applications has always been surrounded by an array of challenges. For example:

1. **Infrastructure Hurdles**: Accessing real-time data often means struggling to acquire data and deploying complex infrastructure, requiring significant time and expertise to get right.

2. **Cumbersome Tools**: Traditional tools for streaming data are bulky, with steep learning curves and complex JVM-based setups.

3. **Analysis Disconnect**: AI models thrive on historical data, but the tools designed for bulk historical analysis are often worlds apart from those made for real-time or streaming data processing.

4. **Challenges of Time-Travel**: AI applications frequently require a unique kind of historical analysis – one that can time-travel through your data. Expressing such analyses is challenging with conventional analytic tools that weren’t designed with time in mind.

These challenges have made it difficult for all but the largest companies with the deepest development budgets to deliver on the promise of real-time AI, and these are the challenges we built Kaskada to solve.

## Welcome to the New Kaskada

We originally built Kaskada as a managed service. Earlier this year, we [released Kaskada as an open-source, self-managed service](./2023-03-28-announcing-kaskada-oss.md), simplifying data onboarding and allowing Kaskada to be deployed anywhere.

Today, we take the next step in improving Kaskada’s usability by providing its core compute engine as an embedded Python library. Because Kaskada is written in Rust, we’re able to leverage the excellent [PyO3](https://pyo3.rs/) project to compile Python-native bindings for our compute engine and support Python-defined UDF’s. Additionally, Kaskada is built using [Apache Arrow](https://arrow.apache.org/), which allows zero-copy data transfers between Kaskada and other Python libraries such as [Pandas](https://pandas.pydata.org/), allowing you to operate on your data in-place.

We’re also changing how you query Kaskada by implementing our query DSL as Python functions. This change makes it easier to get started by eliminating the learning curve of a new language and improving integration with code editors, syntax highlighters, and AI coding assistants.

The result is an easy-to-use Python-native library with all the efficiency and performance of our low-level Rust implementation, fully integrated with the rich Python ecosystem of AI/ML tools, visualization libraries etc.

## Features for Real-Time AI Applications

Real-Time AI is easier today than it's ever been:

* Foundation models built by OpenAI, Facebook and others can be used as a starting point, allowing sophisticated applications to be built with a fraction of the data that would otherwise be necessary.
* Services such as OpenAI eliminate the need to manage complex infrastructure.
* Platforms like HuggingFace have made it easier than ever to share and collaborate on open LLMs.

The New Kaskada complements these resources, making it easier than ever to utilize real-time data by providing several key components:

### 1. Real-time Aggregation

In a world where data is continuously flowing, being able to efficiently precompute model inputs is invaluable. With Kaskada's real-time aggregation, you can effortlessly:

- Connect with multiple data streams using our robust data connectors.
- Transform data on-the-go, ensuring that the model receives the most relevant inputs.
- Perform complex aggregations to derive meaningful insights from streams of data, making sure your AI models always have the most pertinent information.
- Pause and resume aggregations in the event of process termination.

The result? Faster decision-making, timely insights, and AI models that are always a step ahead.

### 2. Event Detection

Real-time event detection can mean the difference between catching an anomaly and letting it slip through the cracks. The New Kaskada’s event detection system is designed to:

- Expressively describe complex cross-event and cross-entity conditions to use as triggers.
- Identify important activities and patterns as they occur, ensuring nothing goes unnoticed.
- Trigger proactive AI behaviors, allowing for immediate actions or notifications based on the detected events.

From spotting fraudulent activities to identifying high-priority user behaviors, Kaskada ensures that important activities are always on your radar.

### 3. History Replay

Past data holds the keys to effective future decisions. With Kaskada's history replay, you can:

- Backtest AI models by revisiting historical data points.
- Fine-tune models using per-example time travel, ensuring your models are always optimized based on past and present data.
- Use point-in-time joins to seamlessly merge data from different data sources at a single point in history, unlocking deeper insights and more accurate predictions.

Kaskada ties together the modern real-time AI stack, providing a data foundation for developing and operating AI applications.

## Join the Community

We believe in the transformative power of real-time AI and the possibilities it holds. We believe that real-time data will allow AI to go beyond question-answering to provide proactive, intelligent applications. We want to hear what excites you about real-time and generative AI - [Join our Slack community](https://kaskada.io/community/) and share your use cases, insights and experiences with the New Kaskada.

*"Real-Time AI without the fuss."* Embrace the future with Kaskada.