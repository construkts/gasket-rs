# Gasket

An opinionated Rust library for building application using data pipeline semantics. It uses a similar approach to the "Actor model", but fine-tuned for heavy, composable data processing.

## Why

- spliting data processing logic into small stages facilitates development (my own experience)
- stages are easily composable
- pipelines are a good abstraction for implementing observability
- robust loops with error handling, backoff retries, etc are ubiquitous and hard to implement correctly
- traditional async loops hide too much of the complexity, hard to optimice at scale

## Goals

- zero-cost abstraction (sort of)
- staticly typed stages, dynamic composition of pipeline at runtime
- developer has full-control over each work unit
- robust work loop provided by the library
- error handling out-of-the-box, with different policies (retry, exit, backoff)
- fully observable out-of-the-box (metrics, traces)
- multi-thread pipeline, async option inside stage
- ergonomic pipeline setup
- back-pressure out-of-the-box
- hot swap of stages (add / remove)

## Status

Current state of the lib is: "only I understand it". I'm experimenting with the api surface, so there are constant breaking changes happening frequently.