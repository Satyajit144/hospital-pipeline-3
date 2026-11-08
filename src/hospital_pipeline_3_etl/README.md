# hospital_pipeline_3

This folder defines all source code for the hospital_pipeline_3 pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample called "sample_trips_hospital_pipeline_3.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run hospital_pipeline_3_etl --refresh sample_trips_hospital_pipeline_3` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.
