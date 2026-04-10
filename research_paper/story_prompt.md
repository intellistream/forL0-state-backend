# SIGMOD Paper Writing Prompts (English)

Goal: write an English SIGMOD paper targeting:
- Data Management Systems: Cloud, distributed, decentralized and parallel data management
- Data Models and Languages: Streams and complex event processing

This document contains:
1) A high-level paper story (what to argue)
2) A set of parallelizable writing tasks (what each agent writes)

---

## Target LaTeX Template (MANDATORY)
All agents must write directly into this LaTeX file and nowhere else:

- Template file: research_paper/acmart-primary/samples/sample-sigconf.tex

If you later switch to a different main .tex file, update the path above and keep the same rules.

Rules:
- Do NOT create new files.
- Do NOT write draft text in chat or separate documents.
- CRITICAL: Before writing any content, you MUST first read and understand the LaTeX template structure, then locate the exact position where your content belongs (e.g., find `\section{Introduction}`, find `\begin{abstract}`, etc.).
- Make edits directly in the target LaTeX file at the correct location.
- If the required section does not exist, create it at the appropriate place following standard paper structure.
- In the final reply, only provide a short confirmation: “Edits applied to the LaTeX template.”

---

## Paper Story (High-Level)

### Positioning
- Setting: large-state stream processing (Apache Flink) faces performance bottlenecks in state access.
- Pain point: state access becomes memory-bound; cache misses dominate throughput and tail latency.
- Research question: how to redesign state storage—without breaking API semantics and snapshot compatibility—to be cache-friendly and reduce access path length and object overhead.

### Problem Statement
- Baseline behavior: random memory access, object fragmentation, and deep object indirection amplify cache miss rates.
- Amplification in streaming: windowed state, timer-heavy workloads, and frequent updates intensify this effect in distributed settings.
- Goal: shift state access from “pointer chasing + deep object graphs” to “compact indexing + locality-aware layout.”

### Challenges and Solutions (Abstract, No Implementation Names)
1) Random access & fragmentation → compact hash indexing + control-byte parallel matching + key inlined layout so probing and key validation are served from the same cache line.
2) Deep object layers → lightweight state storage structure to shorten access chains and reduce indirections.
3) Namespace locality underutilized → hierarchical organization (KeyGroup → Namespace → small sub-table) to concentrate working sets and reduce cache thrashing.

### Evaluation Plan
- Workloads:
	- flink-state-benchmark microbenchmarks (isolated StateBackend testing)
	- benchset end-to-end benchmarks (application-level performance)
- Metrics: throughput, latency, cache miss rate, and memory-bound breakdown.
- Baselines: HashMapStateBackend / HeapStateBackend.
- Platform: Linux; multiple scales; varied key/namespace distributions.

---

# Task Breakdown Prompts (Parallel Writing Tasks)

Each task below is assigned to a different agent. Each agent must ONLY modify the target LaTeX template file: research_paper/acmart-primary/samples/sample-sigconf.tex.

For every task:
- STEP 1: First, read the LaTeX template file to understand its current structure.
- STEP 2: Locate the exact position specified in "Where to edit" (search for the LaTeX command like `\section{...}`, `\begin{abstract}`, `\title{...}`, etc.).
- STEP 3: Write your content at that exact location. If the section doesn't exist, create it in the appropriate place.
- Do not add placeholder TODO text.
- Do not include code identifiers, class names, or file names from the implementation.
- End your reply with exactly: “Edits applied to the LaTeX template.”

---

## Task 1: Title + Abstract
**Objective:** write the paper title and abstract.

**Where to edit (LaTeX):**
- Replace `\title{...}`
- Replace the content inside `\begin{abstract}` ... `\end{abstract}`

**What to write (content requirements):**
- English only.
- Abstract: 150–200 words, single paragraph.
- Cover: memory-bound state access and cache misses; compact indexing + parallel matching + lightweight storage + namespace locality; evaluation using flink-state-benchmark (micro) and benchset (end-to-end); report qualitative improvements (no hard numbers).
- No class names, no code details, no file paths.

---

## Task 2: Introduction
**Objective:** write the full Introduction section.

**Where to edit (LaTeX):**
- Fill/replace the text under `\section{Introduction}` until the next `\section{...}`.

**What to write (content requirements):**
- English only.
- 4–6 paragraphs.
- Include: streaming context; why state access dominates; why cache misses cause memory-bound behavior; what we change; how we evaluate; and why this matters for distributed/parallel stream processing.
- End of Introduction: a bullet list of 3 contributions.
- No implementation names.

---

## Task 3: Problem Statement + Motivation
**Objective:** write a focused problem statement and motivation.

**Where to edit (LaTeX):**
- Add a new subsection under Introduction OR create a new `\section{Background and Motivation}` right after Introduction (pick whichever fits the current template structure).

**What to write (content requirements):**
- English only.
- 2–3 paragraphs for Problem Statement + 2–3 paragraphs for Motivation.
- Explain: random access + fragmentation; deep indirection; why streaming windows/timers and frequent updates exacerbate it; and what properties we seek (locality, compactness, short access paths).
- No code or class names.

---

## Task 4: Design Overview
**Objective:** present the design at a high level.

**Where to edit (LaTeX):**
- Create/overwrite `\section{Design Overview}`.

**What to write (content requirements):**
- English only.
- 3–4 paragraphs + a bullet list of 6–8 design principles.
- Must mention: locality-driven data layout; compact indexing and parallel matching at a high level; lightweight access path; namespace-aware organization; compatibility with state semantics and snapshot/restore.
- No implementation names.

---

## Task 5: Challenges & Solutions
**Objective:** write the three core “challenge → solution” arguments.

**Where to edit (LaTeX):**
- Create/overwrite `\section{Challenges and Solutions}`.

**What to write (content requirements):**
- English only.
- Three paragraphs, one per challenge, 4–6 sentences each.
- Each paragraph must follow: symptom/root cause → design principle → solution overview.
- Must include these three topics:
	1) Random access & fragmentation → compact indexing + control-byte parallel matching + key inlined layout (same cache line)
	2) Deep object layers → lightweight storage to shorten access chains
	3) Namespace locality → KeyGroup → Namespace → small sub-table organization
- No implementation names.

---

## Task 6: System Details
**Objective:** explain the mechanisms without code.

**Where to edit (LaTeX):**
- Create/overwrite `\section{System Details}`.

**What to write (content requirements):**
- English only.
- Four paragraphs (3–5 sentences each), in this order:
	1) Compact indexing + control-byte parallel matching (high-level)
	2) Key inlined layout and cache-line behavior
	3) Lightweight access path with KeyGroup and Namespace routing (conceptual)
	4) Snapshot/restore compatibility (conceptual)
- No implementation names.

---

## Task 7: Evaluation Setup
**Objective:** define workloads, metrics, baselines, and methodology.

**Where to edit (LaTeX):**
- Create/overwrite `\section{Evaluation}` and add a `\subsection{Setup}` inside it.

**What to write (content requirements):**
- English only.
- Include exactly these workloads:
	- flink-state-benchmark microbenchmarks (isolated backend testing)
	- benchset end-to-end benchmarks (application-level performance)
- Metrics: throughput, latency, cache miss rate, and memory-bound breakdown.
- Baselines: HashMapStateBackend and HeapStateBackend.
- Platform: Linux; mention scale and parameter sweeps (state size, key/namespace distribution).
- Add two LaTeX tables (table skeletons):
	1) Workloads and what they stress
	2) Metrics and how they are collected

---

## Task 8: Results & Discussion
**Objective:** write the narrative for results and interpret them.

**Where to edit (LaTeX):**
- Under `\section{Evaluation}`, add `\subsection{Results}` and `\subsection{Discussion}`.

**What to write (content requirements):**
- English only.
- Results: 2–3 paragraphs describing trends (no fabricated numbers).
- Discussion: 1 paragraph connecting improvements to cache misses and reduced memory-bound behavior; note where benefits are strongest (windowed/high-state-pressure).
- Avoid exaggerated claims.

---

## Task 9: Related Work
**Objective:** position against prior work.

**Where to edit (LaTeX):**
- Create/overwrite `\section{Related Work}`.

**What to write (content requirements):**
- English only.
- Three subsections, one paragraph each:
	1) State management in stream processing
	2) Cache-friendly hash table / indexing techniques
	3) Distributed state backends and storage architectures
- Mention citation directions as keywords/areas (no need to add actual BibTeX entries yet).

---

## Task 10: Contributions + Conclusion
**Objective:** finalize contributions and conclusion.

**Where to edit (LaTeX):**
- Create/overwrite `\section{Conclusion}`.
- If the template has a “Contributions” paragraph elsewhere, include contributions at the end of Introduction; otherwise include them at the start of Conclusion.

**What to write (content requirements):**
- English only.
- Start with a bullet list of 3 contributions:
	- Cache-friendly state storage via compact indexing and parallel matching
	- Lightweight state storage that shortens access paths
	- Namespace-aware organization improving locality in distributed streaming
- Then write a 6–8 sentence conclusion paragraph with forward-looking note.

---

## Global Constraints (apply to every task)
- Language: English.
- Venue: SIGMOD.
- Do not mention implementation class names, method names, or source files.
- Do not fabricate experimental numbers.- MANDATORY WORKFLOW:
  1. Read the LaTeX template file first.
  2. Search for the exact location specified in your task (e.g., `\section{Introduction}`, `\begin{abstract}`).
  3. Insert or replace content at that exact location.
  4. Do NOT write content anywhere else (no chat drafts, no new files).- Edits must be applied directly in research_paper/acmart-primary/samples/sample-sigconf.tex.
- Final reply must be exactly: “Edits applied to the LaTeX template.”