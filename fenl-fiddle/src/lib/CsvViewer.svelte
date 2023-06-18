<script lang="ts">
  import {
    Label,
    Table,
    TableBody,
    TableBodyCell,
    TableBodyRow,
    TableHead,
    TableHeadCell,
  } from "flowbite-svelte";

  import * as d3 from "d3";

  export let csvData: string = "";
  export let title: string = "Results:";

  let columnNames: string[] = [];
  let rows: { string: any }[] = [];

  $: {
    rows = d3.csvParse(csvData);
    if (rows.length > 0) {
      columnNames = Object.keys(rows[0]);
    }
    console.log(rows);
  }
</script>

<Label>
  {title}
  <Table class="mt-2">
    <TableHead>
      <TableHeadCell>Index</TableHeadCell>
      {#each columnNames as columnName}
        <TableHeadCell>{columnName}</TableHeadCell>
      {/each}
    </TableHead>
    <TableBody tableBodyClass="divide-y">
      {#each rows as row, index}
        <TableBodyRow>
          <TableBodyCell>{index}</TableBodyCell>
          {#each columnNames as columnName}
            <TableBodyCell>{row[columnName]}</TableBodyCell>
          {/each}
        </TableBodyRow>
      {/each}
    </TableBody>
  </Table>
</Label>
