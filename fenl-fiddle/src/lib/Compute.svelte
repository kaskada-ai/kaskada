<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import DataDrop from "./DataDrop.svelte";
  import SchemaViewer from "./SchemaViewer.svelte";
  import Terminal from "./Terminal.svelte";
  import { Textarea, Label, Tabs, TabItem } from "flowbite-svelte";
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
    if (computable) {
      computeResult = await invoke("compute", {
        csv,
        sourceName,
        entityColumnName,
        timeColumnName,
      });
    }
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

<div class="rounded-lg p-2 bg-gray-200 dark:bg-gray-800 m-2">
  <DataDrop
    bind:sourceName
    bind:timeColumnName
    bind:entityColumnName
    bind:csv
  />
</div>
<div class="rounded-lg p-2 bg-gray-200 dark:bg-gray-800 m-2">
  <div class="flex flex-row gap-2">
    <div class="basis-5/12">
      <Label
        >Query:
        <Textarea
          on:input={compile}
          on:blur={compute}
          id="data-input"
          rows="10"
          class="mt-2"
          placeholder="fenl..."
          bind:value={expression}
          spellcheck="false"
        />
      </Label>
    </div>
    <div class="basis-4/12">
      <Terminal lines={diagnostics} />
    </div>
    <div class="basis-3/12">
      <SchemaViewer schemaFields={schema.fields} />
    </div>
  </div>

  <Tabs
    contentClass="p-1 bg-gray-50 rounded-lg dark:bg-gray-800"
    activeClasses="px-4 py-2 text-gray-900 border-b-2 border-gray-600 dark:text-gray-100 dark:border-gray-400"
    inactiveClasses="px-4 py-2 border-b-2 border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300 text-gray-500 dark:text-gray-400"
  >
    <TabItem title="Results:" disabled />
    <TabItem title="Raw" open>
      <Textarea
        id="data-input"
        value={computeResult}
        class="overscroll-none overflow-auto h-48"
      />
    </TabItem>
    <TabItem title="Table">
      <div class="overscroll-none overflow-auto h-48">
        <CsvViewer csvData={computeResult} />
      </div>
    </TabItem>
  </Tabs>
</div>
