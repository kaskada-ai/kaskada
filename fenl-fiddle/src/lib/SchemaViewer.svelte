<script lang="ts">
  export let schema = {
    fields: [],
  };

  const decodeType = function (field: {
    name: string;
    data_type: { kind: { Primitive: number } };
  }): string {
    switch (field.data_type.kind.Primitive) {
      case 0:
        return "unspecified";
      case 1:
        return "null";
      case 2:
        return "bool";
      case 3:
        return "i8";
      case 4:
        return "i16";
      case 5:
        return "i32";
      case 6:
        return "i64";
      case 7:
        return "u8";
      case 8:
        return "u16";
      case 9:
        return "u32";
      case 10:
        return "u64";
      case 11:
        return "f16";
      case 12:
        return "f32";
      case 13:
        return "f64";
      case 14:
        return "string";
      case 15:
        return "interval_days";
      case 16:
        return "interval_months";
      case 17:
        return "duration_s";
      case 18:
        return "duration_ms";
      case 19:
        return "duration_us";
      case 20:
        return "duration_ns";
      case 21:
        return "timestamp_s";
      case 22:
        return "timestamp_ms";
      case 23:
        return "timestamp_us";
      case 24:
        return "timestamp_ns";
      case 25:
        return "json";
      case 26:
        return "date_32";
      default:
        return "unknown";
    }
  };
</script>

<section>
  <table>
    <tr>
      <td>Field</td>
      <td>Type</td>
    </tr>
    {#each schema.fields as field}
      <tr>
        <td>{field.name}</td>
        <td>{decodeType(field)}</td>
      </tr>
    {/each}
  </table>
</section>

<style>
  table,
  td {
    border: 1px solid;
    border-color: aliceblue;
    border-collapse: collapse;
  }
</style>
