package dpfm_api_input_reader

import (
	"data-platform-api-product-type-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToProductType() *requests.ProductType {
	data := sdc.ProductType
	return &requests.ProductType{
		ProductType: data.ProductType,
	}
}
