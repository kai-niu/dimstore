from nebula.providers.output_render.output_render_base import OutputRenderBase

class HtmlRender(OutputRenderBase):
    
    def __init__(self, config):
        self.config = config

    def store_detail(self, output):
        pass

    def feature_list(self, output):
        pass

    def namespace_list(self, output):
        for ns, count in output:
            