terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = ">= 5.0.0"
    }
  }
}

provider "oci" {
  tenancy_ocid = var.tenancy_ocid
  region       = var.region
}

# ── Variáveis ──────────────────────────────────────────────────────────────────

variable "tenancy_ocid" {}
variable "compartment_ocid" {}
variable "region" { default = "sa-saopaulo-1" }

variable "instance_display_name" { default = "instance-migracao" }
variable "instance_shape"        { default = "VM.Standard.A1.Flex" }
variable "instance_ocpus"        { default = 1 }
variable "instance_memory_in_gbs"{ default = 6 }

variable "ssh_public_key" {
  description = "Chave SSH publica para acesso a instancia"
  default     = ""
}

# ── Dados ──────────────────────────────────────────────────────────────────────

data "oci_identity_availability_domains" "ads" {
  compartment_id = var.tenancy_ocid
}

data "oci_core_images" "ubuntu_arm" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape                    = var.instance_shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

data "oci_core_vcns" "existing" {
  compartment_id = var.compartment_ocid
}

data "oci_core_subnets" "existing" {
  compartment_id = var.compartment_ocid
}

# ── Instância ──────────────────────────────────────────────────────────────────

resource "oci_core_instance" "migracao" {
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  compartment_id      = var.compartment_ocid
  display_name        = var.instance_display_name
  shape               = var.instance_shape

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_in_gbs
  }

  source_details {
    source_type = "image"
    source_id   = data.oci_core_images.ubuntu_arm.images[0].id
  }

  create_vnic_details {
    subnet_id        = data.oci_core_subnets.existing.subnets[0].id
    assign_public_ip = true
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
  }

  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}

# ── Outputs ────────────────────────────────────────────────────────────────────

output "instance_public_ip" {
  value = oci_core_instance.migracao.public_ip
}

output "instance_id" {
  value = oci_core_instance.migracao.id
}
