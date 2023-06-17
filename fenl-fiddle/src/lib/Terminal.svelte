<script lang="ts">
	import { Label } from "flowbite-svelte";
	export let lines: string[];
</script>

<Label
	>Debug:
	<div class="terminal mt-2 bg-gray-300 dark:bg-gray-900">
		{#each lines as line}
			<p class="output text-gray-800 dark:text-gray-200">
				{#each line.split("\n") as outputLine, index}
					{#if index == 0}
						<span class="prompt text-green-500 dark:text-green-400"
							>&gt;</span
						>
					{/if}

					{@html outputLine.replaceAll(" ", "&nbsp;")}<br />
				{/each}
			</p>
		{/each}
	</div>
</Label>

<style>
	.terminal {
		position: relative;
		font-family: monospace;
		font-size: 0.7rem;
		padding: 0.7rem 1rem;
		border-radius: 0.5rem;
		height: 14rem;
		overflow: auto;
		text-align: left;
		line-height: normal;
	}

	.prompt {
		font-weight: bold;
	}

	p {
		margin: 0.2rem 0;
	}
	p.output {
		font-weight: normal;
	}
</style>
