<script lang="ts">
	export let lines: string[];
</script>

<section class="terminal">
	{#each lines as line}
		<p class="output">
			{#each line.split("\n") as outputLine, index}
				{#if index == 0}
					<span class="prompt">&gt;</span>
				{/if}

				{@html outputLine.replaceAll(" ", "&nbsp;")}<br />
			{/each}
		</p>
	{/each}
</section>

<style>
	.terminal {
		position: relative;
		font-family: monospace;
		font-size: 0.7rem;
		background: #333;
		padding: 0.7rem 1rem;
		margin: 5rem;
		border-radius: 0.5rem;
		color: #b8b8b8;
		height: 10rem;
		width: 50em;
		overflow: auto;
		text-align: left;
		line-height: normal;
	}

	.prompt {
		color: #27c93f;
		font-weight: bold;
	}

	p {
		margin: 0.2rem 0;
	}
	p.output {
		font-weight: normal;
	}
</style>
