<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import DataDrop from "./DataDrop.svelte";
  import SchemaViewer from "./SchemaViewer.svelte";
  import Terminal from "./Terminal.svelte";
  import { Textarea, Label, Button } from "flowbite-svelte";
  import CsvViewer from "./CsvViewer.svelte";

  let expression = "";
  let sourceName: string;
  let timeColumnName: string;
  let entityColumnName: string;
  let csv: string;

  let schema: {
    fields: {
      name: string;
      data_type: { kind: { Primitive: number } };
    }[];
  } = { fields: [] };

  let diagnostics: string[] = [];

  let computeResult = "";

  let computable = false;

  async function compute() {
    computeResult = await invoke("compute", {
      csv,
      sourceName,
      entityColumnName,
      timeColumnName,
    });
  }

  async function compile() {
    // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
    let compileResult: {
      fenl_diagnostics: {
        fenl_diagnostics: [
          {
            formatted: string;
          }
        ];
      };
      result_type: {
        kind: {
          Struct: {
            fields: string[];
          };
        };
      };
    } = await invoke("compile", {
      expression,
      sourceName,
      entityColumnName,
      timeColumnName,
    });

    computable = false;

    diagnostics = [];
    if ("fenl_diagnostics" in compileResult) {
      if ("fenl_diagnostics" in compileResult.fenl_diagnostics) {
        for (let element of compileResult.fenl_diagnostics.fenl_diagnostics) {
          diagnostics.push(element.formatted);
        }
      }
    }

    if ("result_type" in compileResult) {
      if (
        "kind" in compileResult.result_type &&
        compileResult.result_type.kind != null
      ) {
        if ("Struct" in compileResult.result_type.kind) {
          schema = compileResult.result_type.kind.Struct;
          computable = true;
        }
      }
    }
  }
</script>

<DataDrop bind:sourceName bind:timeColumnName bind:entityColumnName bind:csv />

<div class="flex flex-row gap-2">
  <div class="basis-5/12">
    <form on:submit|preventDefault={compute}>
      <Label
        >Query:
        <Textarea
          on:keyup={compile}
          on:change={compile}
          id="data-input"
          rows="10"
          class="mt-2"
          placeholder="fenl..."
          bind:value={expression}
          spellcheck="false"
        />
      </Label>
      <Button type="submit" disabled={!computable}>Compute</Button>
    </form>
  </div>
  <div class="basis-4/12">
    <Terminal lines={diagnostics} />
  </div>
  <div class="basis-3/12">
    <SchemaViewer schemaFields={schema.fields} />
  </div>
</div>

<CsvViewer csvData={computeResult} title={"Results:"} />
