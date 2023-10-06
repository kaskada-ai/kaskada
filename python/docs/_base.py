import re

from plum import dispatch



# render -----------------------------------------------------------------------


class BaseRenderer:
    style: str
    _registry: "dict[str, Renderer]" = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls.style in cls._registry:
            raise KeyError(f"A builder for style {cls.style} already exists")

        cls._registry[cls.style] = cls

    @classmethod
    def from_config(cls, cfg: "dict | Renderer | str"):
        if isinstance(cfg, Renderer):
            return cfg
        elif isinstance(cfg, str):
            style, cfg = cfg, {}
        elif isinstance(cfg, dict):
            style = cfg["style"]
            cfg = {k: v for k, v in cfg.items() if k != "style"}
        else:
            raise TypeError(type(cfg))

        if style.endswith(".py"):
            import os
            import sys
            import importlib

            # temporarily add the current directory to sys path and import
            # this ensures that even if we're executing the quartodoc cli,
            # we can import a custom _renderer.py file.
            # it probably isn't ideal, but seems like a more convenient
            # option than requiring users to register entrypoints.
            sys.path.append(os.getcwd())

            try:
                mod = importlib.import_module(style.rsplit(".", 1)[0])
                return mod.Renderer(**cfg)
            finally:
                sys.path.pop()

        subclass = cls._registry[style]
        return subclass(**cfg)

    @dispatch
    def render(self, el):
        raise NotImplementedError(f"render method does not support type: {type(el)}")