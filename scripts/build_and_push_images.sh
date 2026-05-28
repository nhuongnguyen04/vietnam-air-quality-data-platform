#!/bin/bash
# ===================================================================
# Script build và push ảnh Docker lên Registry (Docker Hub)
# Hệ thống: Vietnam Air Quality Data Platform
# ===================================================================

set -e

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REGISTRY="nguyennhuong"
TAG="latest"

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}      BẮT ĐẦU DỰNG VÀ ĐẨY DOCKER IMAGES LÊN DOCKER HUB REGISTRY     ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Kiểm tra đăng nhập Docker Hub
echo -e "${YELLOW}[1/5] Kiểm tra trạng thái đăng nhập Docker Hub...${NC}"
if ! docker info | grep -q "Username"; then
    echo -e "${YELLOW}Cảnh báo: Bạn chưa đăng nhập Docker Hub hoặc Docker daemon chưa chạy.${NC}"
    echo -e "Vui lòng chạy lệnh '${GREEN}docker login${NC}' trước khi chạy script này để đẩy ảnh thành công."
    read -p "Nhấn [Enter] để tiếp tục build cục bộ trước (có thể push sau)..."
fi

# 2. Xây dựng ảnh Airflow Custom
echo -e "\n${YELLOW}[2/5] Đang build và tag Airflow Image...${NC}"
AIRFLOW_IMAGE="${REGISTRY}/vietnam-air-quality-airflow:${TAG}"
echo -e "Đang chạy: docker build -t ${AIRFLOW_IMAGE} -f airflow/Dockerfile ."
docker build -t "${AIRFLOW_IMAGE}" -f airflow/Dockerfile .

# 3. Xây dựng ảnh Streamlit Dashboard
echo -e "\n${YELLOW}[3/5] Đang build và tag Streamlit Dashboard Image...${NC}"
DASHBOARD_IMAGE="${REGISTRY}/vietnam-air-quality-dashboard:${TAG}"
echo -e "Đang chạy: docker build -t ${DASHBOARD_IMAGE} -f python_jobs/dashboard/Dockerfile ."
docker build -t "${DASHBOARD_IMAGE}" -f python_jobs/dashboard/Dockerfile .

# 4. Xây dựng ảnh Text-to-SQL
echo -e "\n${YELLOW}[4/5] Đang build và tag Text-to-SQL AI Assistant Image...${NC}"
TEXT_TO_SQL_IMAGE="${REGISTRY}/vietnam-air-quality-text-to-sql:${TAG}"
echo -e "Đang chạy: docker build -t ${TEXT_TO_SQL_IMAGE} -f python_jobs/text_to_sql/Dockerfile ."
docker build -t "${TEXT_TO_SQL_IMAGE}" -f python_jobs/text_to_sql/Dockerfile .

# 5. Xây dựng ảnh Docker Stats Exporter
echo -e "\n${YELLOW}[5/5] Đang build và tag Docker Stats Exporter Image...${NC}"
STATS_EXPORTER_IMAGE="${REGISTRY}/vietnam-air-quality-stats-exporter:${TAG}"
echo -e "Đang chạy: docker build -t ${STATS_EXPORTER_IMAGE} -f monitoring/docker-stats-exporter/Dockerfile monitoring/docker-stats-exporter"
docker build -t "${STATS_EXPORTER_IMAGE}" -f monitoring/docker-stats-exporter/Dockerfile monitoring/docker-stats-exporter

echo -e "\n${GREEN}=====================================================================${NC}"
echo -e "${GREEN}🎉 HOÀN THÀNH BUILD CÁC DOCKER IMAGES CỤC BỘ THÀNH CÔNG!              ${NC}"
echo -e "   - ${BLUE}${AIRFLOW_IMAGE}${NC}"
echo -e "   - ${BLUE}${DASHBOARD_IMAGE}${NC}"
echo -e "   - ${BLUE}${TEXT_TO_SQL_IMAGE}${NC}"
echo -e "   - ${BLUE}${STATS_EXPORTER_IMAGE}${NC}"
echo -e "${GREEN}=====================================================================${NC}"

# Tiến hành đẩy ảnh lên Docker Hub
read -p "Bạn có muốn đẩy (push) 4 ảnh Docker này lên Docker Hub không? (y/n): " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Đang đẩy các ảnh lên Docker Hub...${NC}"
    docker push "${AIRFLOW_IMAGE}"
    docker push "${DASHBOARD_IMAGE}"
    docker push "${TEXT_TO_SQL_IMAGE}"
    docker push "${STATS_EXPORTER_IMAGE}"
    
    echo -e "${GREEN}🎉 ĐÃ ĐẨY TẤT CẢ CÁC ẢNH LÊN REGISTRY THÀNH CÔNG!${NC}"
else
    echo -e "${YELLOW}Đã bỏ qua đẩy ảnh lên Docker Hub. Các ảnh đã được lưu cục bộ dưới tag ${REGISTRY}/*:${TAG}${NC}"
fi
