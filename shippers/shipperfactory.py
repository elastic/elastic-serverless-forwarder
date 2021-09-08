from shippers import CommonShipper, ElasticsearchShipper


class ShipperFactory:
    _available_targets: list[str] = ["elasticsearch"]
    _init_kwargs_by_target: dict[str, list[str]] = {
        "elasticsearch": ["hosts", "scheme", "username", "password", "index"]
    }

    def __init__(self, target: str, **kwargs: str):
        if target not in self._available_targets:
            raise ValueError(f'''
                you must provide one of the following targets:
                    {", ".join(self._available_targets)}
            ''')

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in self._init_kwargs_by_target[target]]
        if len(init_kwargs) is not len(self._init_kwargs_by_target[target]):
            raise ValueError(f'''
                you must provide the following init kwargs for {target}:
                    {", ".join(self._init_kwargs_by_target[target])}.
                (provided: {", ".join(kwargs.keys())})
            ''')

        if target == "elasticsearch":
            self._shipper: CommonShipper = ElasticsearchShipper(hosts=kwargs["hosts"].split(","),
                                                                username=kwargs["username"],
                                                                password=kwargs["password"],
                                                                scheme=kwargs["scheme"],
                                                                index=kwargs["index"],
                                                                )

    def send(self, event: dict[str, any]):
        self._shipper.send(event)
