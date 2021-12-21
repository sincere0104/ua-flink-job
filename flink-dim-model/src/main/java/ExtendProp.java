public class ExtendProp {
    private String _lib_version;
    private String _os;
    private String _carrier;
    private String _city;
    private String _os_version;
    private String _province;
    private String _title;
    private String _lib;
    private String _app_version;
    private String _country;
    private String _district;
    private String _app_name;
    private String _model;
    private String _network_type;
    private String _url;
    private String _geo;

    public ExtendProp() {
    }

    public ExtendProp(String _lib_version,String _os,String _carrier,String _city,
                      String _os_version,String _province,String _title,String _lib,
                      String _app_version,String _country,String _district,String _app_name,
                      String _model,String _network_type,String _url,String _geo)
    {
        this._lib_version=_lib_version;
        this._carrier=_carrier;
        this._city=_city;
        this._os_version=_os_version;
        this._province=_province;
        this._title=_title;
        this._lib=_lib;
        this._app_version=_app_version;
        this._country=_country;
        this._district=_district;
        this._app_name=_app_name;
        this._network_type=_network_type;
        this._url=_url;
        this._geo=_geo;

        if (_os.isEmpty()) {
            this._os = "未知";
        } else {
            this._os = _os;
        }
        if (_model.isEmpty()) {
            this._model = "未知";
        } else {
            this._model = _model;
        }
    }


    public String getOs() {
        return _os;
    }

    public void setOs(String _os)
    {
        if (_os == null || _os.isEmpty()) {
            this._os = "未知";
        } else {
            this._os = _os;
        }
    }
    public String getModel() {
        return _model;
    }

    public void setModel(String _model)
    {
        if (_model == null || _model.isEmpty()) {
            this._model = "未知";
        } else {
            this._model = _model;
        }
    }


    @Override
    public String toString() {
        return "Properties{" +
                " _os='" + _os + '\'' +
                ", _model='" + _model + '\'' +
                '}';
    }
}
