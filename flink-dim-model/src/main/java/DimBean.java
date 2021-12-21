public class DimBean {
    private String modelId;
    private String modelName;

    public DimBean() { }


    public DimBean(String modelId, String modelName) {
        if (modelId == null || modelId.isEmpty()) {
            this.modelId = "未知";
        } else {
            this.modelId = modelId;
        }

        if (modelName == null || modelName.isEmpty()) {
            this.modelName = "未知";
        } else {
            this.modelName = modelName;
        }
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        if (modelId == null || modelId.isEmpty()) {
            this.modelId = "未知";
        } else {
            this.modelId = modelId;
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        if (modelName == null || modelName.isEmpty()) {
            this.modelName = "未知";
        } else {
            this.modelName = modelName;
        }
    }
}
